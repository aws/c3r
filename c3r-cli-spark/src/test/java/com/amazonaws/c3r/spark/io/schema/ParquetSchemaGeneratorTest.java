// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.schema;

import com.amazonaws.c3r.data.ClientDataType;
import com.amazonaws.c3r.spark.utils.FileTestUtility;
import com.amazonaws.c3r.spark.utils.GeneralTestUtility;
import com.amazonaws.c3r.spark.utils.SparkSessionTestUtility;
import org.apache.spark.SparkException;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ParquetSchemaGeneratorTest {

    private final SparkSession sparkSession = SparkSessionTestUtility.initSparkSession();

    private ParquetSchemaGenerator getTestSchemaGenerator(final String file) throws IOException {
        final String output = FileTestUtility.resolve("schema.json").toString();
        return ParquetSchemaGenerator.builder()
                .inputParquetFile(file)
                .targetJsonFile(output)
                .overwrite(true)
                .sparkSession(sparkSession)
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
        final String emptyParquetFile = FileTestUtility.createTempFile("empty", ".parquet").toString();
        assertThrows(SparkException.class, () ->
                getTestSchemaGenerator(emptyParquetFile));
    }
}