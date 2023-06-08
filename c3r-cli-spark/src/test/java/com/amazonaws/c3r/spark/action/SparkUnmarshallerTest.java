// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.action;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnInsight;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.encryption.keys.KeyUtil;
import com.amazonaws.c3r.json.GsonUtil;
import com.amazonaws.c3r.spark.config.SparkDecryptConfig;
import com.amazonaws.c3r.spark.config.SparkEncryptConfig;
import com.amazonaws.c3r.spark.io.csv.SparkCsvReader;
import com.amazonaws.c3r.spark.io.parquet.SparkParquetReader;
import com.amazonaws.c3r.spark.utils.FileTestUtility;
import com.amazonaws.c3r.spark.utils.SparkSessionTestUtility;
import com.amazonaws.c3r.utils.FileUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static com.amazonaws.c3r.spark.utils.GeneralTestUtility.EXAMPLE_SALT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkUnmarshallerTest {
    private static Dataset<Row> dataset;

    private static List<ColumnInsight> columnInsights;

    private static SparkSession session;

    private static SparkEncryptConfig encryptConfig;

    private static SparkDecryptConfig decryptConfig;


    /**
     * Initial setup done only once because the data is immutable and starting Spark sessions each time is expensive.
     *
     * @throws IOException if Schema can't be read.
     */
    @BeforeAll
    public static void setupDataset() throws IOException {
        final TableSchema schema = GsonUtil.fromJson(FileUtil.readBytes("../samples/schema/config_sample.json"), TableSchema.class);
        columnInsights = schema.getColumns().stream().map(ColumnInsight::new)
                .collect(Collectors.toList());
        session = SparkSessionTestUtility.initSparkSession();
        encryptConfig = SparkEncryptConfig.builder()
                .source("../samples/csv/data_sample_without_quotes.csv")
                .targetDir(FileTestUtility.createTempDir().resolve("output").toString())
                .overwrite(true)
                .secretKey(KeyUtil.sharedSecretKeyFromString(System.getenv(KeyUtil.KEY_ENV_VAR)))
                .salt(EXAMPLE_SALT.toString())
                .tableSchema(schema)
                .settings(ClientSettings.lowAssuranceMode())
                .build();
        decryptConfig = SparkDecryptConfig.builder()
                .source("../samples/csv/marshalled_data_sample.csv")
                .targetDir(FileTestUtility.createTempDir().resolve("output").toString())
                .overwrite(true)
                .secretKey(KeyUtil.sharedSecretKeyFromString(System.getenv(KeyUtil.KEY_ENV_VAR)))
                .salt(EXAMPLE_SALT.toString())
                .build();
        dataset = readDataset(encryptConfig.getSourceFile(), schema.getPositionalColumnHeaders());
    }

    private static Dataset<Row> readDataset(final String sourceFile, final List<ColumnHeader> columnHeaders) {
        return SparkCsvReader.readInput(session,
                sourceFile,
                null,
                columnHeaders);
    }

    @Test
    public void unmarshalDataTest() {
        final Dataset<Row> encryptedData = SparkMarshaller.encrypt(dataset, encryptConfig);
        final List<Row> decryptedData = SparkUnmarshaller.unmarshalData(encryptedData, decryptConfig).collectAsList();

        final Dataset<Row> mappedDataset = SparkMarshaller.mapSourceToTargetColumns(dataset, columnInsights);
        SparkMarshaller.populateColumnPositions(mappedDataset, columnInsights);
        final List<Row> mappedDataList = mappedDataset.collectAsList();

        compareValues(mappedDataList, decryptedData, columnInsights);
    }

    @Test
    public void unmarshalDataParquetUnencryptedMixedTypesTest() {
        final Dataset<Row> mixedDataset = SparkParquetReader
                .readInput(session, "../samples/parquet/data_sample_with_non_string_types.parquet");
        // assert there is indeed a non-String type
        assertTrue(mixedDataset.schema().toList().filter(struct -> struct.dataType() != DataTypes.StringType).size() > 0);

        final Dataset<Row> encryptedData = SparkMarshaller.encrypt(mixedDataset, encryptConfig);
        final List<Row> decryptedData = SparkUnmarshaller.unmarshalData(encryptedData, decryptConfig).collectAsList();

        final Dataset<Row> mappedDataset = SparkMarshaller.mapSourceToTargetColumns(mixedDataset, columnInsights);
        SparkMarshaller.populateColumnPositions(mappedDataset, columnInsights);
        final List<Row> mappedDataList = mappedDataset.collectAsList();

        compareValues(mappedDataList, decryptedData, columnInsights);
    }

    @Test
    public void decryptTest() {
        final Dataset<Row> encryptedData = SparkMarshaller.encrypt(dataset, encryptConfig);
        final List<Row> decryptedData = SparkUnmarshaller.decrypt(encryptedData, decryptConfig).collectAsList();

        final Dataset<Row> mappedDataset = SparkMarshaller.mapSourceToTargetColumns(dataset, columnInsights);
        SparkMarshaller.populateColumnPositions(mappedDataset, columnInsights);
        final List<Row> mappedDataList = mappedDataset.collectAsList();

        compareValues(mappedDataList, decryptedData, columnInsights);
    }

    private void compareValues(final List<Row> expected, final List<Row> actual, final List<ColumnInsight> columnInsights) {
        assertEquals(expected.size(), actual.size());

        // Encryption shuffled the rows
        expected.sort((d1, d2) -> {
            return d2.getString(0).compareTo(d1.getString(0)); //compare on first names
        });
        actual.sort((d1, d2) -> {
            return d2.getString(0).compareTo(d1.getString(0)); //compare on first names
        });

        final List<Integer> fingerprintCols = columnInsights.stream()
                .filter(columnInsight -> columnInsight.getType() == ColumnType.FINGERPRINT)
                .map(ColumnInsight::getSourceColumnPosition)
                .collect(Collectors.toList());

        final List<Integer> decryptableCols = columnInsights.stream()
                .filter(columnInsight -> columnInsight.getType() != ColumnType.FINGERPRINT)
                .map(ColumnInsight::getSourceColumnPosition)
                .collect(Collectors.toList());

        for (int i = 0; i < actual.size(); i++) {
            for (Integer fingerprintPos : fingerprintCols) {
                if (expected.get(i).get(fingerprintPos) == null) {
                    assertNull(actual.get(i).get(fingerprintPos));
                } else {
                    assertNotEquals(expected.get(i).get(fingerprintPos),
                            actual.get(i).get(fingerprintPos));
                }
            }
            for (Integer decryptedPos : decryptableCols) {
                assertEquals(expected.get(i).get(decryptedPos),
                        actual.get(i).get(decryptedPos));

            }
        }
    }
}
