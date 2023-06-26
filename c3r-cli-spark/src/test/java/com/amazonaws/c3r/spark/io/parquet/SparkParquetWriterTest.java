// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.parquet;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.encryption.keys.KeyUtil;
import com.amazonaws.c3r.json.GsonUtil;
import com.amazonaws.c3r.spark.config.SparkEncryptConfig;
import com.amazonaws.c3r.spark.utils.FileTestUtility;
import com.amazonaws.c3r.spark.utils.SparkSessionTestUtility;
import com.amazonaws.c3r.utils.FileUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.amazonaws.c3r.spark.utils.GeneralTestUtility.EXAMPLE_SALT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkParquetWriterTest {
    private static SparkSession session;

    private static SparkEncryptConfig config;

    /**
     * Initial setup done only once because the data is immutable and starting Spark sessions each time is expensive.
     *
     * @throws IOException if Schema can't be read.
     */
    @BeforeAll
    public static void setup() throws IOException {
        final TableSchema schema = GsonUtil.fromJson(FileUtil.readBytes("../samples/schema/config_sample.json"), TableSchema.class);
        session = SparkSessionTestUtility.initSparkSession();
        config = SparkEncryptConfig.builder()
                .source("../samples/parquet/data_sample.parquet")
                .targetDir(FileTestUtility.createTempDir().resolve("output").toString())
                .overwrite(true)
                .secretKey(KeyUtil.sharedSecretKeyFromString(System.getenv(KeyUtil.KEY_ENV_VAR)))
                .salt(EXAMPLE_SALT.toString())
                .tableSchema(schema)
                .settings(ClientSettings.lowAssuranceMode())
                .build();
    }

    @Test
    public void writeOutputTest() {
        final Dataset<Row> originalDataset = SparkParquetReader.readInput(
                session,
                config.getSourceFile());
        final List<String> originalColumns = Arrays.stream(originalDataset.columns())
                .map(String::toLowerCase)
                .sorted()
                .collect(Collectors.toList());
        SparkParquetWriter.writeOutput(originalDataset, config.getTargetFile());
        final Dataset<Row> newDataset = SparkParquetReader.readInput(
                session,
                config.getTargetFile());
        final List<String> newColumns = Arrays.stream(originalDataset.columns())
                .map(String::toLowerCase)
                .sorted()
                .collect(Collectors.toList());
        assertEquals(originalColumns.size(), newColumns.size());
        assertTrue(originalColumns.containsAll(newColumns));

        // Confirm after writing and reading back that no data was lost
        final List<Row> originalDatasetRows = originalDataset.collectAsList();
        final List<Row> newDatasetRows = newDataset.collectAsList();
        assertEquals(originalDatasetRows.size(), newDatasetRows.size());
        assertTrue(originalDatasetRows.containsAll(newDatasetRows));
    }
}
