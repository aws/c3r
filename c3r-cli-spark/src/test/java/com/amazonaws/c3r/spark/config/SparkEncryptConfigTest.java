// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.config;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.MappedTableSchema;
import com.amazonaws.c3r.config.PositionalTableSchema;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.spark.action.SparkMarshaller;
import com.amazonaws.c3r.spark.io.CsvTestUtility;
import com.amazonaws.c3r.spark.io.csv.SparkCsvReader;
import com.amazonaws.c3r.spark.io.csv.SparkCsvWriter;
import com.amazonaws.c3r.spark.utils.FileTestUtility;
import com.amazonaws.c3r.spark.utils.SparkSessionTestUtility;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.amazonaws.c3r.spark.utils.GeneralTestUtility.TEST_CONFIG_DATA_SAMPLE;
import static com.amazonaws.c3r.spark.utils.GeneralTestUtility.TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT;
import static com.amazonaws.c3r.spark.utils.GeneralTestUtility.cleartextColumn;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkEncryptConfigTest {

    private String output;

    private SparkEncryptConfig.SparkEncryptConfigBuilder minimalConfigBuilder(final String sourceFile) {
        return SparkEncryptConfig.builder()
                .secretKey(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getKey())
                .source(sourceFile)
                .targetDir(output)
                .salt(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getSalt())
                .settings(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getSettings())
                .tableSchema(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getSchema());
    }

    // Helper function for calling row marshaller on settings.
    private void runConfig(final SparkEncryptConfig config) {
        final SparkSession session = SparkSessionTestUtility.initSparkSession();
        final Dataset<Row> dataset = SparkCsvReader.readInput(session,
                config.getSourceFile(),
                config.getCsvInputNullValue(),
                config.getTableSchema().getPositionalColumnHeaders());
        final Dataset<Row> marshalledDataset = SparkMarshaller.encrypt(dataset, config);
        SparkCsvWriter.writeOutput(marshalledDataset, config.getTargetFile(), config.getCsvOutputNullValue());
    }

    @BeforeEach
    public void setup() throws IOException {
        output = FileTestUtility.createTempDir().resolve("outputDir").toString();
    }

    @Test
    public void minimumViableConstructionTest() {
        assertDoesNotThrow(() -> minimalConfigBuilder(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getInput())
                .build());
    }

    // Make sure input file must be specified.
    @Test
    public void validateInputBlankTest() {
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> minimalConfigBuilder(
                TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getInput())
                .source("").build());
    }

    @Test
    public void validateOutputEmptyTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> minimalConfigBuilder(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getInput())
                .targetDir("").build());
    }

    @Test
    public void validateNoOverwriteTest() throws IOException {
        output = FileTestUtility.createTempDir().toString();
        assertThrows(C3rIllegalArgumentException.class, () -> minimalConfigBuilder(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getInput())
                .overwrite(false).build());
    }

    @Test
    public void validateOverwriteTest() throws IOException {
        output = FileTestUtility.createTempDir().toString();
        assertDoesNotThrow(() -> minimalConfigBuilder(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getInput())
                .overwrite(true).build());
    }

    @Test
    public void validateEmptySaltTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> minimalConfigBuilder(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getInput())
                .salt("").build());
    }

    @Test
    public void validateFileExtensionWhenInputIsDirectoryTest() {
        assertDoesNotThrow(() -> minimalConfigBuilder(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getInput())
                .source(FileTestUtility.createTempDir().toString())
                .overwrite(true)
                .fileFormat(FileFormat.PARQUET)
                .build());
    }

    @Test
    public void validateNoFileExtensionWhenInputIsDirectoryTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> minimalConfigBuilder(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getInput())
                .source(FileTestUtility.createTempDir().toString())
                .overwrite(true)
                .build());
    }

    @Test
    public void unknownFileExtensionTest() throws IOException {
        final String pathWithUnknownExtension = FileTestUtility.createTempFile("input", ".unknown").toString();

        // unknown extensions cause failure if no FileFormat is specified
        assertThrowsExactly(C3rIllegalArgumentException.class, () ->
                minimalConfigBuilder(pathWithUnknownExtension).build());

        // specifying a FileFormat makes it work
        assertDoesNotThrow(() ->
                minimalConfigBuilder(pathWithUnknownExtension)
                        .fileFormat(FileFormat.CSV)
                        .build());
    }

    @Test
    public void csvOptionsNonCsvFileFormatForFileTest() throws IOException {
        final String parquetPath = FileTestUtility.createTempFile("input", ".parquet").toString();
        // parquet file is fine
        assertDoesNotThrow(() ->
                minimalConfigBuilder(parquetPath).build());

        // parquet file with csvInputNullValue errors
        assertThrowsExactly(C3rIllegalArgumentException.class, () ->
                minimalConfigBuilder(parquetPath)
                        .csvInputNullValue("")
                        .build());

        // parquet file with csvOutputNullValue errors
        assertThrowsExactly(C3rIllegalArgumentException.class, () ->
                minimalConfigBuilder(parquetPath)
                        .csvOutputNullValue("")
                        .build());
    }

    @Test
    public void csvOptionNonCsvFileFormatForDirectoryTest() throws IOException {
        // Use an input directory
        final var config = minimalConfigBuilder(FileTestUtility.createTempDir().toString())
                .overwrite(true)
                .fileFormat(FileFormat.PARQUET);

        // Parquet file format by itself is fine
        assertDoesNotThrow(() -> config.build());

        // Parquet format with an input CSV null value specified is not accepted
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> config.csvInputNullValue("NULL").build());

        // Parquet format with an output CSV null value specified is not accepted
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> config.csvOutputNullValue("NULL").build());
    }

    // Make sure positional schema and file that are equivalent to file and schema with headers.
    @Test
    public void noHeaderFileProducesCorrectResultsTest() throws IOException {
        final String noHeadersFile = "../samples/csv/data_sample_no_headers.csv";
        final TableSchema noHeadersSchema = new PositionalTableSchema(List.of(
                List.of(cleartextColumn(null, "FirstName")),
                List.of(cleartextColumn(null, "LastName")),
                List.of(cleartextColumn(null, "Address")),
                List.of(cleartextColumn(null, "City")),
                List.of(cleartextColumn(null, "State")),
                List.of(cleartextColumn(null, "PhoneNumber")),
                List.of(cleartextColumn(null, "Title")),
                List.of(cleartextColumn(null, "Level")),
                List.of(cleartextColumn(null, "Notes"))
        ));
        final String headersFile = "../samples/csv/data_sample_without_quotes.csv";
        final TableSchema headersSchema = new MappedTableSchema(List.of(
                cleartextColumn("FirstName"),
                cleartextColumn("LastName"),
                cleartextColumn("Address"),
                cleartextColumn("City"),
                cleartextColumn("State"),
                cleartextColumn("PhoneNumber"),
                cleartextColumn("Title"),
                cleartextColumn("Level"),
                cleartextColumn("Notes")
        ));

        final SparkEncryptConfig noHeadersConfig = SparkEncryptConfig.builder()
                .source(noHeadersFile)
                .targetDir(FileTestUtility.createTempDir().resolve("encryptedNoHeaders").toString())
                .overwrite(true)
                .csvInputNullValue(null)
                .csvOutputNullValue(null)
                .secretKey(TEST_CONFIG_DATA_SAMPLE.getKey())
                .salt(TEST_CONFIG_DATA_SAMPLE.getSalt())
                .settings(TEST_CONFIG_DATA_SAMPLE.getSettings())
                .tableSchema(noHeadersSchema)
                .build();
        runConfig(noHeadersConfig);
        final Path mergedNoHeadersOutput = CsvTestUtility.mergeOutput(Path.of(noHeadersConfig.getTargetFile()));

        final SparkEncryptConfig headersConfig = SparkEncryptConfig.builder()
                .source(headersFile)
                .targetDir(FileTestUtility.createTempDir().resolve("encryptedHeaders").toString())
                .overwrite(true)
                .csvInputNullValue(null)
                .csvOutputNullValue(null)
                .secretKey(TEST_CONFIG_DATA_SAMPLE.getKey())
                .salt(TEST_CONFIG_DATA_SAMPLE.getSalt())
                .settings(TEST_CONFIG_DATA_SAMPLE.getSettings())
                .tableSchema(headersSchema)
                .build();
        runConfig(headersConfig);

        final Path mergedHeadersOutput = CsvTestUtility.mergeOutput(Path.of(headersConfig.getTargetFile()));
        final List<String> noHeaderLines = Files.readAllLines(mergedNoHeadersOutput);
        final List<String> headerLines = Files.readAllLines(mergedHeadersOutput);
        assertEquals(headerLines.size(), noHeaderLines.size());
        noHeaderLines.sort(String::compareTo);
        headerLines.sort(String::compareTo);
        for (int i = 0; i < headerLines.size(); i++) {
            assertEquals(0, headerLines.get(i).compareTo(noHeaderLines.get(i)));
        }
    }

    // Make sure custom null values work with positional schemas.
    @Test
    public void customNullValueWithPositionalSchemaTest() throws IOException {
        final String noHeadersFile = "../samples/csv/data_sample_no_headers.csv";
        final TableSchema noHeadersSchema = new PositionalTableSchema(List.of(
                List.of(cleartextColumn(null, "FirstName")),
                List.of(cleartextColumn(null, "LastName")),
                List.of(cleartextColumn(null, "Address")),
                List.of(cleartextColumn(null, "City")),
                List.of(cleartextColumn(null, "State")),
                List.of(cleartextColumn(null, "PhoneNumber")),
                List.of(cleartextColumn(null, "Title")),
                List.of(cleartextColumn(null, "Level")),
                List.of(cleartextColumn(null, "Notes"))
        ));
        final SparkEncryptConfig noHeadersConfig = SparkEncryptConfig.builder()
                .source(noHeadersFile)
                .targetDir(output)
                .overwrite(true)
                .csvInputNullValue("John")
                .csvOutputNullValue("NULLJOHNNULL")
                .secretKey(TEST_CONFIG_DATA_SAMPLE.getKey())
                .salt(TEST_CONFIG_DATA_SAMPLE.getSalt())
                .settings(TEST_CONFIG_DATA_SAMPLE.getSettings())
                .tableSchema(noHeadersSchema)
                .build();
        runConfig(noHeadersConfig);
        final Path mergedNoHeadersOutput = CsvTestUtility.mergeOutput(Path.of(noHeadersConfig.getTargetFile()));
        final List<String> noHeaderLines = Files.readAllLines(mergedNoHeadersOutput);
        boolean foundNull = false;
        for (String row : noHeaderLines) {
            foundNull |= row.startsWith("NULLJOHNNULL,Smith");
        }
        assertTrue(foundNull);
    }

    // Check that validation fails because cleartext columns aren't allowed but cleartext columns are in the schema.
    @Test
    void checkAllowCleartextValidationTest() {
        final String noHeadersFile = "../samples/csv/data_sample_no_headers.csv";
        final TableSchema schema = new MappedTableSchema(List.of(cleartextColumn("cleartext")));
        final var config = SparkEncryptConfig.builder()
                .source(noHeadersFile)
                .targetDir(output)
                .overwrite(true)
                .secretKey(TEST_CONFIG_DATA_SAMPLE.getKey())
                .salt(TEST_CONFIG_DATA_SAMPLE.getSalt())
                .tableSchema(schema);
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class,
                () -> config.settings(ClientSettings.highAssuranceMode()).build());
        assertEquals("Cleartext columns found in the schema, but allowCleartext is false. Target column names: [`cleartext`]",
                e.getMessage());
        assertDoesNotThrow(() -> config.settings(ClientSettings.lowAssuranceMode()).build());
    }
}
