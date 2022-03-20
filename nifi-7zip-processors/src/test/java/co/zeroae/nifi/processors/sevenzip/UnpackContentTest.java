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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;



@RunWith(Parameterized.class)
public class UnpackContentTest {
    // Inspired by TestUnpackContent
    // https://github.com/apache/nifi/blob/main/nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/test/java/org/apache/nifi/processors/standard/TestUnpackContent.java
    private static final Path dataPath = Paths.get("src/test/resources");

    private TestRunner testRunner;

    final private String fileName;
    final private Integer expectedFileCount;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(UnpackContent.class);
    }

    public UnpackContentTest(String fileName, Integer expectedFileCount) {
        this.fileName = fileName;
        this.expectedFileCount = expectedFileCount;
    }

    @Parameterized.Parameters
    public static Collection inputFiles() {
        return Arrays.asList(new Object[][] {
                {"simple.txt.gz", 1},
                {"simple.zip", 3},
                {"simple.7z", 4},
                {"simple.iso", 2},
        });
    }

    @Test
    public void testProcessor() throws IOException {
        // Prepare
        testRunner.enqueue(dataPath.resolve(this.fileName));

        // Act
        testRunner.run();

        //Assert
        testRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(UnpackContent.REL_SUCCESS, this.expectedFileCount);

        final MockFlowFile original = testRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS).get(0);
    }
}
