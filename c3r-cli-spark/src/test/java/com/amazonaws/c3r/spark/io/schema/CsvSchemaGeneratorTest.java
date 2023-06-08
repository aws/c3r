// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.schema;

import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.spark.utils.FileTestUtility;
import com.amazonaws.c3r.utils.FileUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static com.amazonaws.c3r.spark.utils.GeneralTestUtility.DATA_SAMPLE_HEADERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CsvSchemaGeneratorTest {

    private CsvSchemaGenerator getTestSchemaGenerator(final String file) throws IOException {
        final String output = FileTestUtility.resolve("schema.json").toString();
        return CsvSchemaGenerator.builder()
                .inputCsvFile(file)
                .hasHeaders(true)
                .targetJsonFile(output)
                .overwrite(true)
                .build();
    }

    @Test
    public void getSourceHeadersTest() throws IOException {
        assertEquals(
                DATA_SAMPLE_HEADERS,
                getTestSchemaGenerator(FileUtil.CURRENT_DIR + "/../samples/csv/data_sample_without_quotes.csv").getSourceHeaders());
    }

    @Test
    public void getSourceColumnCountTest() throws IOException {
        assertEquals(
                DATA_SAMPLE_HEADERS.size(),
                getTestSchemaGenerator(FileUtil.CURRENT_DIR + "/../samples/csv/data_sample_without_quotes.csv").getSourceColumnCount());
    }

    @Test
    public void emptyFileTest() throws IOException {
        final Path emptyCsvFile = FileTestUtility.createTempFile("empty", ".csv");
        assertThrows(C3rRuntimeException.class, () ->
                getTestSchemaGenerator(emptyCsvFile.toString()));
    }
}
