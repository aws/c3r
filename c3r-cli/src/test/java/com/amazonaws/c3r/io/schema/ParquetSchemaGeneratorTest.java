// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io.schema;

import com.amazonaws.c3r.data.ClientDataType;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.utils.FileTestUtility;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ParquetSchemaGeneratorTest {

    private ParquetSchemaGenerator getTestSchemaGenerator(final String file) throws IOException {
        final String output = FileTestUtility.resolve("schema.json").toString();
        return ParquetSchemaGenerator.builder()
                .inputParquetFile(file)
                .targetJsonFile(output)
                .overwrite(true)
                .build();
    }

    @Test
    public void getSourceHeadersTest() throws IOException {
        assertEquals(
                GeneralTestUtility.DATA_SAMPLE_HEADERS,
                getTestSchemaGenerator("../samples/parquet/data_sample.parquet").getSourceHeaders());
    }

    @Test
    public void getSourceColumnCountTest() throws IOException {
        assertEquals(
                Collections.nCopies(GeneralTestUtility.DATA_SAMPLE_HEADERS.size(), ClientDataType.STRING),
                getTestSchemaGenerator("../samples/parquet/data_sample.parquet").getSourceColumnTypes());
    }

    @Test
    public void getSourceColumnTypesTest() throws IOException {
        assertEquals(
                List.of(ClientDataType.BOOLEAN,
                        ClientDataType.STRING,
                        ClientDataType.UNKNOWN,
                        ClientDataType.SMALLINT,
                        ClientDataType.INT,
                        ClientDataType.BIGINT,
                        ClientDataType.FLOAT,
                        ClientDataType.DOUBLE,
                        ClientDataType.TIMESTAMP),
                getTestSchemaGenerator("../samples/parquet/rows_100_groups_10_prim_data.parquet").getSourceColumnTypes());
    }

    @Test
    public void emptyFileTest() throws IOException {
        final String emptyParquetFile = FileTestUtility.createTempFile("empty", ".parquet").toString();
        assertThrows(C3rRuntimeException.class, () ->
                getTestSchemaGenerator(emptyParquetFile));
    }
}