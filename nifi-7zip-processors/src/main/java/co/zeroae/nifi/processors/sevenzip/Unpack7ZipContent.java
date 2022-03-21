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
package co.zeroae.nifi.processors.sevenzip;

import net.sf.sevenzipjbinding.*;
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

import java.io.*;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.List;

@Tags({"7zip", "zip", "tar", "split", "lzma", "iso"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class Unpack7ZipContent extends AbstractProcessor {

    private static final String OCTET_STREAM = "application/octet-stream";

    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();
    public static final String SEGMENT_ORIGINAL_FILENAME = FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key();

    public static final String ARCHIVE_FORMAT_AUTO = "auto detect";
    public static final String ARCHIVE_FORMAT_MIME_TYPE = "use mime.type";

    public static final String FILE_LAST_MODIFIED_TIME_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_CREATION_TIME_ATTRIBUTE = "file.creationTime";
    public static final String FILE_OWNER_ATTRIBUTE = "file.owner";
    public static final String FILE_GROUP_ATTRIBUTE = "file.group";
    public static final String FILE_PERMISSIONS_ATTRIBUTE = "file.permissions";
    public static final String FILE_ENCRYPTION_METHOD_ATTRIBUTE = "file.encryptionMethod";

    public static final String FILE_MODIFIED_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(FILE_MODIFIED_DATE_ATTR_FORMAT).withZone(ZoneId.systemDefault());

    public static final PropertyDescriptor ARCHIVE_FORMAT = new PropertyDescriptor.Builder()
            .name("ARCHIVE_FORMAT")
            .displayName("Archive Format")
            .description("The Archive Format used to create the file.")
            .required(true)
            .allowableValues(getArchiveFormats())
            .defaultValue(ARCHIVE_FORMAT_AUTO)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Unpacked FlowFiles are sent to this relationship")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .autoTerminateDefault(true)
            .description("The original FlowFile is sent to this relationship after it has been successfully unpacked")
            .build();
    public static final Relationship REL_EMPTY = new Relationship.Builder()
            .name("empty")
            .autoTerminateDefault(true)
            .description("The original FlowFile is sent to this relationship if it is empty")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("The original FlowFile is sent to this relationship when it cannot be unpacked for some reason")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    static String[] getArchiveFormats() {
        ArrayList<String> rv = new ArrayList<>();
        rv.add(ARCHIVE_FORMAT_AUTO);
        for (ArchiveFormat format : ArchiveFormat.values()) {
            rv.add(format.name().toLowerCase(Locale.ROOT));
        }
        return rv.toArray(new String[0]);
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(ARCHIVE_FORMAT);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_EMPTY);
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

        final ArchiveFormat archiveFormat;
        switch (context.getProperty(ARCHIVE_FORMAT).getValue()) {
            case ARCHIVE_FORMAT_AUTO:
                archiveFormat= null;
                break;
            case ARCHIVE_FORMAT_MIME_TYPE:
                // TODO: Implement mime.type to ARCHIVE_FORMAT conversion
                archiveFormat= null;
                logger.warn("mime.type to archive format is not implemented yet. Reverting to auto.");
                break;
            default:
                archiveFormat= ArchiveFormat.valueOf(context
                        .getProperty(ARCHIVE_FORMAT)
                        .getValue()
                        .toUpperCase(Locale.ROOT)
                );
                break;
        }

        final FlowFileExtractor extractor = new FlowFileExtractor(getLogger(), session, archiveFormat);
        extractor.extract();
    }


    /**
     * Apply split index and count attributes.
     *
     * @param session session
     * @param splits splits
     */
    private void finishFragmentAttributes(final ProcessSession session, final List<FlowFile> splits) {
        final ArrayList<FlowFile> newList = new ArrayList<>(splits);
        splits.clear();
        for (int i = 1; i <= newList.size(); i++) {
            FlowFile ff = newList.get(i - 1);
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(FRAGMENT_INDEX, String.valueOf(i));
            attributes.put(FRAGMENT_COUNT, String.valueOf(newList.size()));
            FlowFile newFF = session.putAllAttributes(ff, attributes);
            splits.add(newFF);
        }
    }

    static private class FlowFileInStream implements IInStream {
        final ProcessSession session;
        final FlowFile flowFile;
        final long currentStreamSize;

        InputStream currentInputStream;
        long currentPosition;

        public FlowFileInStream(ProcessSession session, FlowFile flowFile) {
            this.session = session;
            this.flowFile = flowFile;
            this.currentStreamSize = flowFile.getSize();

            this.currentInputStream = this.session.read(this.flowFile);
            this.currentPosition = 0;
        }

        private void resetInputStream() throws SevenZipException {
            try {
                close();
            } catch (IOException e) {
                throw new SevenZipException(e.getMessage(), e.getCause());
            }
            this.currentInputStream = this.session.read(this.flowFile);
            this.currentPosition = 0;
        }

        @Override
        synchronized
        public long seek(long offset, int seekOrigin) throws SevenZipException {
            try {
                switch (seekOrigin) {
                    case SEEK_SET:
                        if (offset < currentPosition)
                            resetInputStream();
                        // NOTE: This is the code in Java 12 skipNBytes
                        long n = offset - currentPosition;
                        while (n > 0) {
                            long ns = currentInputStream.skip(n);
                            if (ns > 0 && ns <= n)
                                n -= ns;
                            else if (ns == 0) {
                                if (currentInputStream.read() == -1)
                                    throw new EOFException();
                                n--;
                            } else // negative skip or too many bytes
                                throw new IOException("Unable to skip exactly");
                        }
                        currentPosition = offset;
                        return currentPosition;
                    case SEEK_CUR:
                        return seek(currentPosition + offset, SEEK_SET);
                    case SEEK_END:
                        return seek(currentStreamSize + offset, SEEK_SET);
                }
            } catch (IOException e) {
                throw new SevenZipException(e.getMessage(), e.getCause());
            }
            return 0;
        }

        @Override
        synchronized
        public int read(byte[] data) throws SevenZipException {
            try {
                int read = this.currentInputStream.read(data);
                currentPosition += read;
                return read;
            } catch (IOException e) {
               throw new SevenZipException(e.getMessage(), e.getCause());
            }
        }

        @Override
        public void close() throws IOException {
            if (currentInputStream != null) {
                currentInputStream.close();
                currentInputStream = null;
                currentPosition = Long.MAX_VALUE;
            }
        }
    }

    private class FlowFileExtractor implements IArchiveExtractCallback, Closeable {
        private final ComponentLog logger;
        private final ProcessSession session;
        private final ArchiveFormat archiveFormat;

        private final ArrayList<FlowFile> outFlowFiles;

        private FlowFile inFlowFile;
        private IInArchive inArchive;
        private String fragmentId;
        private int index;
        private long total;

        private boolean skipExtract;
        private FlowFile outFlowFile;
        private OutputStream outputStream;


        public FlowFileExtractor(ComponentLog logger, ProcessSession session, ArchiveFormat archiveFormat) {
            this.logger = logger;
            this.session = session;
            this.archiveFormat = archiveFormat;

            this.outFlowFiles = new ArrayList<>();
        }

        public void extract() {
            outFlowFiles.clear();

            inFlowFile = session.get();
            if (inFlowFile == null)
                return;

            fragmentId = UUID.randomUUID().toString();
            try (FlowFileInStream inputStream = new FlowFileInStream(session, inFlowFile)) {
                try (IInArchive archive = SevenZip.openInArchive(archiveFormat, inputStream)) {
                    // A little weird, but needed since inArchive is used during the callback.
                    this.inArchive = archive;
                    inArchive.extract(null, false, this);
                    finishFragmentAttributes(session, outFlowFiles);
                    session.transfer(outFlowFiles, REL_SUCCESS);
                } finally {
                    inArchive = null;
                }
            } catch (IOException e) {
                logger.error("Unable to unpack {}; transferred to failure relationship.", inFlowFile, e);
                session.remove(this.outFlowFiles);
                session.transfer(inFlowFile, REL_FAILURE);
                this.outFlowFiles.clear();
                return;
            }

            inFlowFile = FragmentAttributes.copyAttributesToOriginal(session, inFlowFile, fragmentId, outFlowFiles.size());
            Relationship dest = outFlowFiles.isEmpty() ? REL_EMPTY : REL_ORIGINAL;
            session.transfer(inFlowFile, dest);
            logger.info("Unpacked {} into {} and transferred to {}", inFlowFile, outFlowFiles, dest);
        }

        @Override
        public ISequentialOutStream getStream(int index, ExtractAskMode extractAskMode) throws SevenZipException {
            this.index = index;
            skipExtract = (boolean) inArchive.getProperty(index, PropID.IS_FOLDER);
            if (skipExtract || extractAskMode != ExtractAskMode.EXTRACT)
                return null;

            outFlowFile = createOutFlowFile();
            outputStream = session.write(outFlowFile);

            return data -> {
                try {
                    outputStream.write(data);
                    return data.length;
                } catch (IOException e) {
                    throw new SevenZipException(e.getMessage(), e.getCause());
                }
            };
        }

        @Override
        public void prepareOperation(ExtractAskMode extractAskMode) {
            logger.debug("Prepare to {} {}", extractAskMode, outFlowFile);
        }

        @Override
        public void setOperationResult(ExtractOperationResult extractOperationResult) throws SevenZipException {
            if (skipExtract)
                return;
            logger.debug("{} operation result: {}", outFlowFile, extractOperationResult);

            try {
                close();
            } catch (IOException e) {
                extractOperationResult = ExtractOperationResult.UNKNOWN_OPERATION_RESULT;
                throw new SevenZipException(e.getMessage(), e.getCause());
            } finally {
                if (extractOperationResult == ExtractOperationResult.OK)
                    outFlowFiles.add(outFlowFile);
                else {
                    // TODO: What to do when it fails?
                    //       1 - Cancel entire operation, route original to failure ?
                    //       2 - Continue with extraction ?
                    session.remove(outFlowFile);
                }
                outFlowFile = null;
            }
        }

        private FlowFile createOutFlowFile() throws SevenZipException {
            // TODO: This is all ugly, refactor if possible

            FlowFile rv = session.create(inFlowFile);
            final Map<String, String> attributes = new HashMap<>();

            // Step zero,
            final String originalFilename = inFlowFile.getAttribute(CoreAttributes.FILENAME.key());
            attributes.put(FRAGMENT_ID, fragmentId);
            attributes.put(SEGMENT_ORIGINAL_FILENAME, originalFilename);

            // Step one, set the mime.type to OCTET_STREAM
            attributes.put(CoreAttributes.MIME_TYPE.key(), OCTET_STREAM);

            // Step two: Fill in the Filename/Path/Absolute Path
            // TODO: convert tgz -> tar, taz -> tar, tbz2 -> tar, etc IFF stringProperty is null or empty.
            String stringProperty = (String) inArchive.getProperty(index, PropID.PATH);
            final File file = new File(
                    !(stringProperty == null || stringProperty.isEmpty())
                            ? stringProperty
                            : originalFilename.substring(0, originalFilename.lastIndexOf('.'))
            );

            final Path filePath = file.toPath();
            attributes.put(CoreAttributes.FILENAME.key(), filePath.toString());

            final String filePathString = filePath.getParent() == null ? "/" : filePath.getParent() + "/";
            attributes.put(CoreAttributes.PATH.key(), filePathString);

            final Path absFilePath = filePath.toAbsolutePath();
            final String absPathString = absFilePath.getParent().toString() + "/";
            attributes.put(CoreAttributes.ABSOLUTE_PATH.key(), absPathString);

            // Step three: USER, GROUP, Dates and Times
            stringProperty = (String) inArchive.getProperty(index, PropID.USER);
            if (stringProperty != null && !stringProperty.isEmpty())
                attributes.put(FILE_OWNER_ATTRIBUTE, stringProperty);

            stringProperty = (String) inArchive.getProperty(index, PropID.GROUP);
            if (stringProperty != null && !stringProperty.isEmpty())
                attributes.put(FILE_GROUP_ATTRIBUTE, stringProperty);

            // TODO: FileInfo lives in nifi.standard.processors, we need to figure out how to import it.
            // attributes.put(FILE_PERMISSIONS_ATTRIBUTE, FileInfo.permissionToString(item.getAttributes()));

            Date dateProperty = (Date) inArchive.getProperty(index, PropID.CREATION_TIME);
            if (dateProperty != null)
                attributes.put(FILE_CREATION_TIME_ATTRIBUTE, DATE_TIME_FORMATTER.format(dateProperty.toInstant()));

            dateProperty = (Date) inArchive.getProperty(index, PropID.LAST_MODIFICATION_TIME);
            if (dateProperty != null)
                attributes.put(FILE_LAST_MODIFIED_TIME_ATTRIBUTE, DATE_TIME_FORMATTER.format(dateProperty.toInstant()));

            return session.putAllAttributes(rv, attributes);
        }

        @Override
        public void setTotal(long total) {
            // Total in Bytes
            this.total = total;
        }

        @Override
        public void setCompleted(long complete) {
            // Complete in Bytes
            logger.debug("[{}/{}] completed...", complete, total);
        }

        @Override
        public void close() throws IOException {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } finally {
                    outputStream = null;
                }
            }
        }
    }
}
