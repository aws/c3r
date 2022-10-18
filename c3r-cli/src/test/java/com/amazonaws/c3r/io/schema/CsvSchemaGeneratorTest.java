// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io.schema;

import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.utils.FileUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.amazonaws.c3r.utils.GeneralTestUtility.DATA_SAMPLE_HEADERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CsvSchemaGeneratorTest {
    private Path tempDir;

    private Path outFile;

    @BeforeEach
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("temp");
        tempDir.toFile().deleteOnExit();
        outFile = tempDir.resolve("schema.json");
        outFile.toFile().deleteOnExit();
    }

    private CsvSchemaGenerator getTestSchemaGenerator(final String file) {
        return CsvSchemaGenerator.builder()
                .inputCsvFile(file)
                .hasHeaders(true)
                .targetJsonFile(outFile.toString())
                .overwrite(true)
                .build();
    }

    @Test
    public void getSourceHeadersTest() {
        assertEquals(
                DATA_SAMPLE_HEADERS,
                getTestSchemaGenerator(FileUtil.CURRENT_DIR + "/../samples/csv/data_sample_without_quotes.csv").getSourceHeaders());
    }

    @Test
    public void getSourceColumnCountTest() {
        assertEquals(
                DATA_SAMPLE_HEADERS.size(),
                getTestSchemaGenerator(FileUtil.CURRENT_DIR + "/../samples/csv/data_sample_without_quotes.csv").getSourceColumnCount());
    }

    @Test
    public void emptyFileTest() throws IOException {
        final Path emptyCsvFile = Files.createTempFile(tempDir, "empty", "csv");
        emptyCsvFile.toFile().deleteOnExit();
        assertThrows(C3rRuntimeException.class, () ->
                getTestSchemaGenerator(emptyCsvFile.toString()));
    }
}
