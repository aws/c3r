// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io.schema;

import com.amazonaws.c3r.data.ClientDataType;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ParquetSchemaGeneratorTest {
    private Path tempDir;

    @BeforeEach
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("temp");
        tempDir.toFile().deleteOnExit();
    }

    private ParquetSchemaGenerator getTestSchemaGenerator(final String file) {
        final Path outFile = tempDir.resolve("schema.json");
        outFile.toFile().deleteOnExit();
        return ParquetSchemaGenerator.builder()
                .inputParquetFile(file)
                .targetJsonFile(outFile.toString())
                .overwrite(true)
                .build();
    }

    @Test
    public void getSourceHeadersTest() {
        assertEquals(
                GeneralTestUtility.DATA_SAMPLE_HEADERS,
                getTestSchemaGenerator("../samples/parquet/data_sample.parquet").getSourceHeaders());
    }

    @Test
    public void getSourceColumnCountTest() {
        assertEquals(
                Collections.nCopies(GeneralTestUtility.DATA_SAMPLE_HEADERS.size(), ClientDataType.STRING),
                getTestSchemaGenerator("../samples/parquet/data_sample.parquet").getSourceColumnTypes());
    }

    @Test
    public void getSourceColumnTypesTest() {
        assertEquals(
                List.of(ClientDataType.UNKNOWN,
                        ClientDataType.STRING,
                        ClientDataType.UNKNOWN,
                        ClientDataType.UNKNOWN,
                        ClientDataType.UNKNOWN,
                        ClientDataType.UNKNOWN,
                        ClientDataType.UNKNOWN,
                        ClientDataType.UNKNOWN,
                        ClientDataType.UNKNOWN),
                getTestSchemaGenerator("../samples/parquet/rows_100_groups_10_prim_data.parquet").getSourceColumnTypes());
    }

    @Test
    public void emptyFileTest() throws IOException {
        final Path emptyParquetFile = Files.createTempFile(tempDir, "empty", "parquet");
        emptyParquetFile.toFile().deleteOnExit();
        assertThrows(C3rRuntimeException.class, () ->
                getTestSchemaGenerator(emptyParquetFile.toString()));
    }
}