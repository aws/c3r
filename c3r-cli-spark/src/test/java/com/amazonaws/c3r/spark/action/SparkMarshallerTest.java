// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.action;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnInsight;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.MappedTableSchema;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.encryption.keys.KeyUtil;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.Limits;
import com.amazonaws.c3r.json.GsonUtil;
import com.amazonaws.c3r.spark.config.SparkEncryptConfig;
import com.amazonaws.c3r.spark.io.csv.SparkCsvReader;
import com.amazonaws.c3r.spark.io.parquet.SparkParquetReader;
import com.amazonaws.c3r.spark.utils.FileTestUtility;
import com.amazonaws.c3r.spark.utils.SparkSessionTestUtility;
import com.amazonaws.c3r.utils.FileUtil;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.c3r.spark.utils.GeneralTestUtility.EXAMPLE_SALT;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkMarshallerTest {
    private static Dataset<Row> dataset;

    private static List<ColumnInsight> columnInsights;

    private static SparkSession session;

    private static TableSchema schema;

    private static SparkEncryptConfig config;

    /**
     * Initial setup done only once because the data is immutable and starting Spark sessions each time is expensive.
     *
     * @throws IOException if Schema can't be read.
     */
    @BeforeAll
    public static void setupDataset() throws IOException {
        schema = GsonUtil.fromJson(FileUtil.readBytes("../samples/schema/config_sample.json"), TableSchema.class);
        columnInsights = schema.getColumns().stream().map(ColumnInsight::new)
                .collect(Collectors.toList());
        session = SparkSessionTestUtility.initSparkSession();
        config = SparkEncryptConfig.builder()
                .source("../samples/csv/data_sample_without_quotes.csv")
                .targetDir(FileTestUtility.createTempDir().resolve("output").toString())
                .overwrite(true)
                .secretKey(KeyUtil.sharedSecretKeyFromString(System.getenv(KeyUtil.KEY_ENV_VAR)))
                .salt(EXAMPLE_SALT.toString())
                .tableSchema(schema)
                .settings(ClientSettings.lowAssuranceMode())
                .build();
        dataset = readDataset(config.getSourceFile(), schema.getPositionalColumnHeaders());
    }

    private static Dataset<Row> readDataset(final String sourceFile, final List<ColumnHeader> columnHeaders) {
        return SparkCsvReader.readInput(session,
                sourceFile,
                null,
                columnHeaders);
    }

    @Test
    public void filterSourceColumnsBySchemaNoMatchesTest() {
        final List<ColumnInsight> emptyColumnInsights = new ArrayList<>();
        final Dataset<Row> filteredDataset = SparkMarshaller.filterSourceColumnsBySchema(dataset, emptyColumnInsights);
        assertEquals(0, filteredDataset.columns().length);
    }

    @Test
    public void filterSourceColumnsBySchemaAllMatchesTest() {
        final Dataset<Row> filteredDataset = SparkMarshaller.filterSourceColumnsBySchema(dataset, columnInsights);
        final Set<String> sourceHeaders = columnInsights.stream()
                .map(columnInsight -> columnInsight.getSourceHeader().toString()).collect(Collectors.toSet());
        assertEquals(sourceHeaders.size(), filteredDataset.columns().length);
    }

    @Test
    public void filterSourceColumnsBySchemaSomeMatchesTest() {
        final List<ColumnInsight> trimmedColumnInsights = schema.getColumns().stream().map(ColumnInsight::new)
                .filter(columnInsight -> columnInsight.getType() == ColumnType.CLEARTEXT)
                .collect(Collectors.toList());
        final Dataset<Row> filteredDataset = SparkMarshaller.filterSourceColumnsBySchema(dataset, trimmedColumnInsights);
        assertNotEquals(trimmedColumnInsights.size(), columnInsights.size());
        assertEquals(trimmedColumnInsights.size(), filteredDataset.columns().length);
    }

    @Test
    public void updateMaxValuesPerColumnTest() {
        SparkMarshaller.updateMaxValuesPerColumn(dataset, columnInsights);
        final Map<String, ColumnInsight> targetToColumnInsight = columnInsights.stream()
                .collect(Collectors.toMap(insight -> insight.getTargetHeader().toString(), insight -> insight));
        // Asert all have a value set since there are no empty columns
        assertFalse(columnInsights.stream().anyMatch(insight -> insight.getMaxValueLength() <= 0));

        final int longestFirstnameByteLength = 5;
        final int longestPhonenumberByteLength = 12;
        final int longestNoteValueByteLength = 60;

        if (!FileUtil.isWindows()) {
            // Spot check our lengths ONLY on *nix system CI. On Windows the Java string literals appearing in the
            // source code can end up getting encoded non-UTF8 initially, which then can muck with the length in
            // annoying ways that just make the tests harder to write in a cross-platform way.
            // NOTE: This concern is only relevant for the spot checks where we check string literal lengths
            // (which feature Java string literals), because the actual application and tests run on external
            // file data are operating only on bytes parsed in as UTF8 from a file REGARDLESS of the OS.
            assertEquals(longestFirstnameByteLength, "Shana".getBytes(StandardCharsets.UTF_8).length);
            assertEquals(longestPhonenumberByteLength, "407-555-8888".getBytes(StandardCharsets.UTF_8).length);
            // Importantly, the longest `Notes` string has a unicode character `é` (U+00E9) that takes two bytes
            // in UTF8 (0xC3 0xA9), and so relying on non-UTF8-byte-length notions of a string value's "length"
            // can lead to errors on UTF8 data containing such values.
            assertEquals(
                    longestNoteValueByteLength,
                    "This is a really long noté that could really be a paragraph"
                            .getBytes(StandardCharsets.UTF_8).length);
        }

        assertEquals(longestFirstnameByteLength, targetToColumnInsight.get("firstname").getMaxValueLength());
        assertEquals(longestPhonenumberByteLength, targetToColumnInsight.get("phonenumber_cleartext").getMaxValueLength());
        assertEquals(longestPhonenumberByteLength, targetToColumnInsight.get("phonenumber_sealed").getMaxValueLength());
        assertEquals(longestNoteValueByteLength, targetToColumnInsight.get("notes").getMaxValueLength());
    }

    @Test
    public void updateMaxValuesPerColumnEmptyDatasetTest() {
        final TableSchema schema = GsonUtil.fromJson(FileUtil.readBytes("../samples/schema/6column.json"), TableSchema.class);
        final List<ColumnInsight> columnInsights = schema.getColumns().stream().map(ColumnInsight::new)
                .collect(Collectors.toList());
        final Dataset<Row> emptyDataset = readDataset("../samples/csv/null5by6.csv", schema.getPositionalColumnHeaders());

        SparkMarshaller.updateMaxValuesPerColumn(emptyDataset, columnInsights);

        // Asert all have a max length of 0
        assertFalse(columnInsights.stream().anyMatch(insight -> insight.getMaxValueLength() != 0));
    }

    @Test
    public void validateDuplicatesAllowDuplicatesTrueTest() {
        final ColumnSchema lastNameSchema = ColumnSchema.builder()
                .sourceHeader(new ColumnHeader("lastname")) // Column has duplicate last names
                .type(ColumnType.FINGERPRINT).build();
        final TableSchema schema = new MappedTableSchema(List.of(lastNameSchema));
        final List<ColumnInsight> lastNameInsights = schema.getColumns().stream().map(ColumnInsight::new)
                .collect(Collectors.toList());
        assertDoesNotThrow(() -> SparkMarshaller.validateDuplicates(ClientSettings.lowAssuranceMode(), dataset, lastNameInsights));
    }

    @Test
    public void validateDuplicatesAllowDuplicatesFalseNonFingerprintColumnTest() {
        final ColumnSchema lastNameSchema = ColumnSchema.builder()
                .sourceHeader(new ColumnHeader("lastname")) // Column has duplicate last names
                .type(ColumnType.CLEARTEXT).build();
        final TableSchema schema = new MappedTableSchema(List.of(lastNameSchema));
        final List<ColumnInsight> lastNameInsights = schema.getColumns().stream().map(ColumnInsight::new)
                .collect(Collectors.toList());
        assertDoesNotThrow(() -> SparkMarshaller.validateDuplicates(ClientSettings.highAssuranceMode(), dataset, lastNameInsights));
    }

    @Test
    public void validateDuplicatesAllowDuplicatesFalseDuplicateValuesTest() {
        final ColumnSchema lastNameSchema = ColumnSchema.builder()
                .sourceHeader(new ColumnHeader("lastname")) // Column has duplicate last names
                .type(ColumnType.FINGERPRINT).build();
        final TableSchema schema = new MappedTableSchema(List.of(lastNameSchema));
        final List<ColumnInsight> lastNameInsights = schema.getColumns().stream().map(ColumnInsight::new)
                .collect(Collectors.toList());
        assertThrows(C3rRuntimeException.class, () -> SparkMarshaller.validateDuplicates(ClientSettings.highAssuranceMode(), dataset,
                lastNameInsights));
    }

    @Test
    public void validateDuplicatesAllowDuplicatesFalseDuplicateNullsTest() {
        final ColumnSchema lastNameSchema = ColumnSchema.builder()
                .sourceHeader(new ColumnHeader("notes")) // Column has duplicate nulls
                .type(ColumnType.FINGERPRINT).build();
        final TableSchema schema = new MappedTableSchema(List.of(lastNameSchema));
        final List<ColumnInsight> lastNameInsights = schema.getColumns().stream().map(ColumnInsight::new)
                .collect(Collectors.toList());
        assertThrows(C3rRuntimeException.class, () -> SparkMarshaller.validateDuplicates(ClientSettings.highAssuranceMode(), dataset,
                lastNameInsights));
    }

    @Test
    public void mapSourceToTargetColumnsTest() {
        final Set<String> sourceColumns = columnInsights.stream()
                .map(columnInsight -> columnInsight.getSourceHeader().toString())
                .collect(Collectors.toSet());
        final Set<String> datasetColumns = Arrays.stream(dataset.columns()).map(String::toLowerCase).collect(Collectors.toSet());

        // assert initial state
        assertEquals(sourceColumns, datasetColumns);

        final Dataset<Row> mappedDataset = SparkMarshaller.mapSourceToTargetColumns(dataset, columnInsights);
        final Set<String> targetColumns = columnInsights.stream()
                .map(columnInsight -> columnInsight.getTargetHeader().toString())
                .collect(Collectors.toSet());

        // assert there are differences
        assertNotEquals(sourceColumns, targetColumns);

        final Set<String> mappedDatasetColumns =
                Arrays.stream(mappedDataset.columns()).map(String::toLowerCase).collect(Collectors.toSet());

        //assert final state
        assertEquals(targetColumns, mappedDatasetColumns);
    }

    @Test
    public void mapSourceToTargetColumnsSqlHeaderMaxLengthTest() {
        final ColumnHeader maxLengthSqlHeader = new ColumnHeader("a".repeat(Limits.AWS_CLEAN_ROOMS_HEADER_MAX_LENGTH));
        final ColumnSchema lastNameSchema =
                ColumnSchema.builder()
                        .sourceHeader(new ColumnHeader("lastname")) // Column has duplicate last names
                        .targetHeader(maxLengthSqlHeader)
                        .type(ColumnType.FINGERPRINT).build();

        final TableSchema schema = new MappedTableSchema(List.of(lastNameSchema));
        final List<ColumnInsight> lastNameInsights = schema.getColumns().stream().map(ColumnInsight::new)
                .collect(Collectors.toList());
        final Dataset<Row> mappedDataset = SparkMarshaller.mapSourceToTargetColumns(dataset, lastNameInsights);

        // Ensure that SparkSQL handles the longest headers that are permitted and doesn't introduce shorter limits.
        assertEquals(maxLengthSqlHeader.toString(), mappedDataset.columns()[0]);
    }

    @Test
    public void populateColumnPositionsTest() {
        final Dataset<Row> mappedDataset = SparkMarshaller.mapSourceToTargetColumns(dataset, columnInsights);
        SparkMarshaller.populateColumnPositions(mappedDataset, columnInsights);
        final Map<String, ColumnInsight> targetToColumnInsight = columnInsights.stream()
                .collect(Collectors.toMap(insight -> insight.getTargetHeader().toString(), insight -> insight));
        // Asert all have a value set since there are no empty columns
        assertFalse(columnInsights.stream().anyMatch(insight -> insight.getSourceColumnPosition() < 0));

        // Spot checks
        assertEquals(0, targetToColumnInsight.get("firstname").getSourceColumnPosition());
        assertEquals(5, targetToColumnInsight.get("phonenumber_cleartext").getSourceColumnPosition());
        assertEquals(6, targetToColumnInsight.get("phonenumber_sealed").getSourceColumnPosition());
        assertEquals(10, targetToColumnInsight.get("notes").getSourceColumnPosition());
    }

    @Test
    public void shuffleDataTest() {
        final List<Row> shuffledData = SparkMarshaller.shuffleData(dataset).collectAsList();
        final List<Row> originalData = dataset.collectAsList();

        assertTrue(shuffledData.containsAll(originalData));
        assertTrue(originalData.containsAll(shuffledData));

        // It's possible the shuffling resulted in some rows in the same place given a short test file, but unlikely that several rows
        // got the same position.
        assertTrue(shuffledData.get(0) != originalData.get(0)
                || shuffledData.get(1) != originalData.get(1)
                || shuffledData.get(2) != originalData.get(2)
                || shuffledData.get(3) != originalData.get(3)
                || shuffledData.get(4) != originalData.get(4)
                || shuffledData.get(5) != originalData.get(5)
                || shuffledData.get(6) != originalData.get(6));
    }

    @Test
    public void marshalDataTest() {
        final Dataset<Row> mappedDataset = SparkMarshaller.mapSourceToTargetColumns(dataset, columnInsights);
        SparkMarshaller.populateColumnPositions(mappedDataset, columnInsights);
        final List<Row> marshalledData = SparkMarshaller.marshalData(mappedDataset, config, columnInsights).collectAsList();
        final List<Row> mappedDataList = mappedDataset.collectAsList();

        // Marshalling doesn't shuffle, so we can compare each row
        compareValues(mappedDataList, marshalledData, columnInsights);
    }

    @Test
    public void marshalDataParquetUnencryptedMixedTypesTest() {
        final Dataset<Row> mixedDataset = SparkParquetReader
                .readInput(session, "../samples/parquet/data_sample_with_non_string_types.parquet");
        // assert there is indeed a non-String type
        assertTrue(mixedDataset.schema().toList().filter(struct -> struct.dataType() != DataTypes.StringType).size() > 0);

        final Dataset<Row> mappedDataset = SparkMarshaller.mapSourceToTargetColumns(mixedDataset, columnInsights);
        SparkMarshaller.populateColumnPositions(mappedDataset, columnInsights);
        final List<Row> marshalledData = SparkMarshaller.marshalData(mappedDataset, config, columnInsights).collectAsList();
        final List<Row> mappedDataList = mappedDataset.collectAsList();

        // Marshalling doesn't shuffle, so we can compare each row
        compareValues(mappedDataList, marshalledData, columnInsights);
    }

    @Test
    public void marshalDataParquetEncryptedMixedTypesTest() {
        final Dataset<Row> mixedDataset = SparkParquetReader
                .readInput(session, "../samples/parquet/data_sample_with_non_string_types.parquet", /* skipHeaderNormalization */ false);
        // assert there is indeed a non-String type
        assertTrue(mixedDataset.schema().toList().filter(struct -> struct.dataType() != DataTypes.StringType).size() > 0);

        final ColumnSchema levelSchema = ColumnSchema.builder()
                .sourceHeader(new ColumnHeader("level")) // Column is classified as an int
                .type(ColumnType.FINGERPRINT).build();
        final TableSchema schema = new MappedTableSchema(List.of(levelSchema));
        final List<ColumnInsight> levelInsights = schema.getColumns().stream().map(ColumnInsight::new)
                .collect(Collectors.toList());

        final Dataset<Row> mappedDataset = SparkMarshaller.mapSourceToTargetColumns(mixedDataset, levelInsights);
        SparkMarshaller.populateColumnPositions(mappedDataset, levelInsights);

        assertThrows(SparkException.class, () -> SparkMarshaller.marshalData(mappedDataset, config, levelInsights).collectAsList());
    }

    @Test
    public void encryptTest() {
        final List<Row> encryptedData = SparkMarshaller.encrypt(dataset, config).collectAsList();

        final Dataset<Row> mappedDataset = SparkMarshaller.mapSourceToTargetColumns(dataset, columnInsights);
        SparkMarshaller.populateColumnPositions(mappedDataset, columnInsights);
        final List<Row> mappedDataList = mappedDataset.collectAsList();

        // Marshalling shuffles, so we need to sort before we can compare each row
        encryptedData.sort((d1, d2) -> {
            return d2.getString(0).compareTo(d1.getString(0)); //compare on first names
        });
        mappedDataList.sort((d1, d2) -> {
            return d2.getString(0).compareTo(d1.getString(0)); //compare on first names
        });

        compareValues(mappedDataList, encryptedData, columnInsights);
    }

    private void compareValues(final List<Row> expected, final List<Row> actual, final List<ColumnInsight> columnInsights) {
        assertEquals(expected.size(), actual.size());

        final List<Integer> ciphertextCols =
                columnInsights.stream().filter(columnInsight -> columnInsight.getType() != ColumnType.CLEARTEXT)
                        .map(ColumnInsight::getSourceColumnPosition)
                        .collect(Collectors.toList());

        final List<Integer> cleartextCols = columnInsights.stream().filter(columnInsight -> columnInsight.getType() == ColumnType.CLEARTEXT)
                .map(ColumnInsight::getSourceColumnPosition)
                .collect(Collectors.toList());

        for (int i = 0; i < actual.size(); i++) {
            for (Integer ciphertextPos : ciphertextCols) {
                if (expected.get(i).get(ciphertextPos) == null) {
                    assertNull(actual.get(i).get(ciphertextPos));
                } else {
                    assertNotEquals(expected.get(i).get(ciphertextPos),
                            actual.get(i).get(ciphertextPos));
                }
            }
            for (Integer cleartextPos : cleartextCols) {
                assertEquals(expected.get(i).get(cleartextPos),
                        actual.get(i).get(cleartextPos));
            }
        }
    }
}
