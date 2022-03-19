/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.scraawl.nifi.processors.sevenzip;

import net.sf.sevenzipjbinding.*;
import net.sf.sevenzipjbinding.simple.ISimpleInArchive;
import net.sf.sevenzipjbinding.simple.ISimpleInArchiveItem;
import net.sf.sevenzipjbinding.util.ByteArrayStream;
import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.*;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Tags({"7zip", "zip", "tar", "split", "lzma", "iso"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class UnpackContent extends AbstractProcessor {

    private static final String OCTET_STREAM = "application/octet-stream";

    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();
    public static final String SEGMENT_ORIGINAL_FILENAME = FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key();

    public static final String FILE_LAST_MODIFIED_TIME_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_CREATION_TIME_ATTRIBUTE = "file.creationTime";
    public static final String FILE_OWNER_ATTRIBUTE = "file.owner";
    public static final String FILE_GROUP_ATTRIBUTE = "file.group";
    public static final String FILE_PERMISSIONS_ATTRIBUTE = "file.permissions";
    public static final String FILE_ENCRYPTION_METHOD_ATTRIBUTE = "file.encryptionMethod";

    public static final String FILE_MODIFIED_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(FILE_MODIFIED_DATE_ATTR_FORMAT).withZone(ZoneId.systemDefault());

    public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor
            .Builder().name("MY_PROPERTY")
            .displayName("My property")
            .description("Example Property")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Unpacked FlowFiles are sent to this relationship")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile is sent to this relationship after it has been successfully unpacked")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("The original FlowFile is sent to this relationship when it cannot be unpacked for some reason")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(MY_PROPERTY);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGINAL);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnAdded
    public void initLibrary() throws SevenZipNativeInitializationException {
        SevenZip.initSevenZipFromPlatformJAR();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        // Code largely inspired by UnpackContent.java
        // https://github.com/apache/nifi/blob/main/nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/UnpackContent.java
        ComponentLog logger = getLogger();
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        final List<FlowFile> unpacked = new ArrayList<>();
        try {
            unpack(session, flowFile, unpacked);
            if (unpacked.isEmpty()) {
                logger.error("Unable to unpack {} because it does not appear to have any entries; routing to failure", flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            // Fragment Attributes
            finishFragmentAttributes(session, flowFile, unpacked);
            final String fragmentId = unpacked.size() > 0 ? unpacked.get(0).getAttribute(FRAGMENT_ID) : null;
            flowFile = FragmentAttributes.copyAttributesToOriginal(session, flowFile, fragmentId, unpacked.size());

            // Transfer
            session.transfer(unpacked, REL_SUCCESS);
            session.transfer(flowFile, REL_ORIGINAL);
            session.getProvenanceReporter().fork(flowFile, unpacked);
            logger.info("Unpacked {} into {} and transferred to success", flowFile, unpacked);
        } catch (Exception e) {
            logger.error("Unable to unpack {}; routing to failure", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            session.remove(unpacked);
        }
    }

    private void unpack(ProcessSession session, FlowFile sourceFlowFile, List<FlowFile> unpacked) {
        // http://sevenzipjbind.sourceforge.net/extraction_snippets.html
        session.read(sourceFlowFile, inputStream -> {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            StreamUtils.copy(inputStream, baos);
            ByteArrayStream bas = new ByteArrayStream(baos.toByteArray(), false);

            IInArchive inArchive = SevenZip.openInArchive(null, bas);
            ISimpleInArchive  iSimpleInArchive = inArchive.getSimpleInterface();

            for (ISimpleInArchiveItem item: iSimpleInArchive.getArchiveItems()) {
                if (item.isFolder()) {
                    continue;
                }
                FlowFile unpackedFile = session.create(sourceFlowFile);
                final File file = new File(item.getPath());
                final Path filePath = file.toPath();
                final String filePathString = filePath.getParent() == null ? "/" : filePath.getParent() + "/";
                final Path absFilePath = filePath.toAbsolutePath();
                final String absPathString = absFilePath.getParent().toString() + "/";

                try {
                    final Map<String, String> attributes = new HashMap<>();
                    attributes.put(CoreAttributes.FILENAME.key(), item.getPath());
                    attributes.put(CoreAttributes.PATH.key(), filePathString);
                    attributes.put(CoreAttributes.ABSOLUTE_PATH.key(), absPathString);
                    attributes.put(CoreAttributes.MIME_TYPE.key(), OCTET_STREAM);

                    if (item.getUser() != null)
                        attributes.put(FILE_OWNER_ATTRIBUTE, item.getUser());
                    if (item.getGroup() != null)
                        attributes.put(FILE_GROUP_ATTRIBUTE, item.getGroup());
                    // TODO: FileInfo lives in nifi.standard.processors, we need to figure out how to import it.
                    // attributes.put(FILE_PERMISSIONS_ATTRIBUTE, FileInfo.permissionToString(item.getAttributes()));
                    if (item.getCreationTime() != null)
                        attributes.put(FILE_CREATION_TIME_ATTRIBUTE, DATE_TIME_FORMATTER.format(item.getCreationTime().toInstant()));
                    if (item.getLastWriteTime() != null)
                        attributes.put(FILE_LAST_MODIFIED_TIME_ATTRIBUTE, DATE_TIME_FORMATTER.format(item.getLastWriteTime().toInstant()));

                    unpackedFile = session.putAllAttributes(unpackedFile, attributes);

                    session.write(unpackedFile, outputStream -> {
                        // TODO: Use the extractCallBack because item by item is Slow
                        ExtractOperationResult result = item.extractSlow(new ISequentialOutStream() {
                            @Override
                            public int write(byte[] data) throws SevenZipException {
                                try {
                                    outputStream.write(data);
                                    return data.length;
                                } catch (IOException e) {
                                    throw new SevenZipException(e.getMessage(), e.getCause());
                                }
                            }
                        });
                        // TODO:  what to do if extract fails?
                        assert(result == ExtractOperationResult.OK);
                    });
                } finally {
                    unpacked.add(unpackedFile);
                }
            }
        });
    }

    /**
     * Apply split index, count and other attributes.
     *
     * @param session session
     * @param source source
     * @param splits splits
     * @return generated fragment identifier for the splits
     */
    private String finishFragmentAttributes(final ProcessSession session, final FlowFile source, final List<FlowFile> splits) {
        final String originalFilename = source.getAttribute(CoreAttributes.FILENAME.key());

        final String fragmentId = UUID.randomUUID().toString();
        final ArrayList<FlowFile> newList = new ArrayList<>(splits);
        splits.clear();
        for (int i = 1; i <= newList.size(); i++) {
            FlowFile ff = newList.get(i - 1);
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(FRAGMENT_ID, fragmentId);
            attributes.put(FRAGMENT_INDEX, String.valueOf(i));
            attributes.put(FRAGMENT_COUNT, String.valueOf(newList.size()));
            attributes.put(SEGMENT_ORIGINAL_FILENAME, originalFilename);
            FlowFile newFF = session.putAllAttributes(ff, attributes);
            splits.add(newFF);
        }
        return fragmentId;
    }
}
