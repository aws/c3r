// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cli;

import com.amazonaws.c3r.cleanrooms.CleanRoomsDaoTestUtility;
import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.utils.FileTestUtility;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import com.amazonaws.c3r.utils.StringTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class SchemaModeTest {
    private static final int SAMPLE_DATA_COLUMN_COUNT = 9;

    private static final String ALL_COLUMN_TYPES =
            "[" + Arrays.stream(ColumnType.values())
                    .map(ColumnType::toString)
                    .collect(Collectors.joining("|")) + "]";

    private static final String ALL_COLUMN_TYPES_SANS_CLEARTEXT =
            "[" + Arrays.stream(ColumnType.values())
                    .filter(c -> c != ColumnType.CLEARTEXT)
                    .map(ColumnType::toString)
                    .collect(Collectors.joining("|")) + "]";

    private Path schemaPath;

    @BeforeEach
    public void setup() throws IOException {
        schemaPath = FileTestUtility.resolve("schema.json");
    }

    // Generate a template without settings and shallowly check content contains expected entries
    private void runTemplateGeneratorNoSettings(final String inputFile,
                                                final boolean hasHeaderRow) throws IOException {
        final var args = SchemaCliConfigTestUtility.builder()
                .input(inputFile)
                .output(schemaPath.toString())
                .subMode("--template")
                .noHeaders(!hasHeaderRow)
                .overwrite(true)
                .build();

        assertEquals(0, SchemaMode.getApp(null).execute(args.toArrayWithoutMode()));

        assertTrue(Files.exists(schemaPath));
        assertTrue(Files.size(schemaPath) > 0);
        final String contents = Files.readString(schemaPath);
        assertTrue(contents.contains("\"headerRow\": " + hasHeaderRow));
        assertEquals(hasHeaderRow ? SAMPLE_DATA_COLUMN_COUNT : 0,
                StringTestUtility.countMatches("sourceHeader", contents));
        assertEquals(SAMPLE_DATA_COLUMN_COUNT,
                StringTestUtility.countMatches(ALL_COLUMN_TYPES, contents));
    }

    // Generate a template with permissive settings and shallowly check content contains expected entries
    private void runTemplateGeneratorPermissiveSettings(final String inputFile,
                                                        final boolean hasHeaderRow) throws IOException {
        final var args = SchemaCliConfigTestUtility.builder()
                .input(inputFile)
                .output(schemaPath.toString())
                .subMode("--template")
                .noHeaders(!hasHeaderRow)
                .overwrite(true)
                .collaborationId(GeneralTestUtility.EXAMPLE_SALT.toString())
                .build();
        final var cleanRoomsDao = CleanRoomsDaoTestUtility.generateMockDao();
        when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(ClientSettings.lowAssuranceMode());

        assertEquals(0, SchemaMode.getApp(cleanRoomsDao).execute(args.toArrayWithoutMode()));

        assertTrue(Files.exists(schemaPath));
        assertTrue(Files.size(schemaPath) > 0);
        final String contents = Files.readString(schemaPath);
        assertTrue(contents.contains("\"headerRow\": " + hasHeaderRow));
        assertEquals(hasHeaderRow ? SAMPLE_DATA_COLUMN_COUNT : 0,
                StringTestUtility.countMatches("sourceHeader", contents));
        assertEquals(SAMPLE_DATA_COLUMN_COUNT,
                StringTestUtility.countMatches(ALL_COLUMN_TYPES, contents));
    }

    // Generate a template with restrictive settings and shallowly check content contains expected entries
    private void runTemplateGeneratorRestrictiveSettings(final String inputFile,
                                                         final int expectedTargetColumnCount,
                                                         final boolean hasHeaderRow) throws IOException {
        final var args = SchemaCliConfigTestUtility.builder()
                .input(inputFile)
                .output(schemaPath.toString())
                .subMode("--template")
                .noHeaders(!hasHeaderRow)
                .overwrite(true)
                .collaborationId(GeneralTestUtility.EXAMPLE_SALT.toString())
                .build();
        final var cleanRoomsDao = CleanRoomsDaoTestUtility.generateMockDao();
        when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(ClientSettings.highAssuranceMode());

        assertEquals(0, SchemaMode.getApp(cleanRoomsDao).execute(args.toArrayWithoutMode()));

        assertTrue(Files.exists(schemaPath));
        assertTrue(Files.size(schemaPath) > 0);
        final String contents = Files.readString(schemaPath);
        assertTrue(contents.contains("\"headerRow\": " + hasHeaderRow));
        assertEquals(hasHeaderRow ? expectedTargetColumnCount : 0,
                StringTestUtility.countMatches("sourceHeader", contents));
        assertEquals(expectedTargetColumnCount,
                StringTestUtility.countMatches("targetHeader", contents));
        assertEquals(expectedTargetColumnCount,
                StringTestUtility.countMatches(ALL_COLUMN_TYPES_SANS_CLEARTEXT, contents));
    }

    // Run interactive schema gen without settings and check it returns results
    // and shallowly check content contains expected entries
    private void runInteractiveGeneratorNoSettings(final String inputFile,
                                                   final boolean hasHeaderRow) throws IOException {
        final var args = SchemaCliConfigTestUtility.builder()
                .input(inputFile)
                .output(schemaPath.toString())
                .subMode("--interactive")
                .noHeaders(!hasHeaderRow)
                .overwrite(true)
                .build();
        // number greater than test file column counts (test will fail if too low, so no incorrectness risk in
        // picking a number)
        final int columnCountUpperBound = 100;
        // user input which repeatedly says the source column in question should generate one cleartext column
        // with a trivial name
        final StringBuilder inputBuilder = new StringBuilder();
        for (int i = 0; i < columnCountUpperBound; i++) {
            // 1 target column
            inputBuilder.append("1\n");
            // target column type
            inputBuilder.append("cleartext\n");
            // target column name
            inputBuilder.append("column").append(i).append('\n');
        }
        final var userInput = new ByteArrayInputStream(inputBuilder.toString().getBytes(StandardCharsets.UTF_8));
        System.setIn(new BufferedInputStream(userInput));

        assertEquals(0, Main.getApp().execute(args.toArray()));

        assertTrue(schemaPath.toFile().exists());
        assertTrue(schemaPath.toFile().length() > 0);
        final String contents = Files.readString(schemaPath);
        assertTrue(contents.contains("\"headerRow\": " + hasHeaderRow));
        assertEquals(hasHeaderRow ? SAMPLE_DATA_COLUMN_COUNT : 0,
                StringTestUtility.countMatches("sourceHeader", contents));
        assertEquals(SAMPLE_DATA_COLUMN_COUNT,
                StringTestUtility.countMatches("\"" + ColumnType.CLEARTEXT + "\"", contents));
    }

    // Run interactive schema gen with permissive settings and check it returns results
    // and shallowly check content contains expected entries

    private void runInteractiveGeneratorPermissiveSettings(final String inputFile,
                                                           final boolean hasHeaderRow) throws IOException {
        final var args = SchemaCliConfigTestUtility.builder()
                .input(inputFile)
                .output(schemaPath.toString())
                .subMode("--interactive")
                .noHeaders(!hasHeaderRow)
                .overwrite(true)
                .collaborationId(GeneralTestUtility.EXAMPLE_SALT.toString())
                .build();
        // number greater than test file column counts (test will fail if too low, so no incorrectness risk in
        // picking a number)
        final int columnCountUpperBound = 100;
        // user input which repeatedly says the source column in question should generate one cleartext column
        // with a trivial name
        final StringBuilder inputBuilder = new StringBuilder();
        for (int i = 0; i < columnCountUpperBound; i++) {
            // 1 target column
            inputBuilder.append("1\n");
            // target column type
            inputBuilder.append("cleartext\n");
            // target column name
            inputBuilder.append("column").append(i).append('\n');
        }
        final var userInput = new ByteArrayInputStream(inputBuilder.toString().getBytes(StandardCharsets.UTF_8));
        System.setIn(new BufferedInputStream(userInput));

        final var cleanRoomsDao = CleanRoomsDaoTestUtility.generateMockDao();
        when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(ClientSettings.lowAssuranceMode());

        assertEquals(0, SchemaMode.getApp(cleanRoomsDao).execute(args.toArrayWithoutMode()));

        assertTrue(schemaPath.toFile().exists());
        assertTrue(schemaPath.toFile().length() > 0);
        final String contents = Files.readString(schemaPath);
        assertTrue(contents.contains("\"headerRow\": " + hasHeaderRow));
        assertEquals(hasHeaderRow ? SAMPLE_DATA_COLUMN_COUNT : 0,
                StringTestUtility.countMatches("sourceHeader", contents));
        assertEquals(SAMPLE_DATA_COLUMN_COUNT,
                StringTestUtility.countMatches("\"" + ColumnType.CLEARTEXT + "\"", contents));
    }

    // Run interactive schema gen with restrictive settings and check it returns results
    // and shallowly check content contains expected entries=
    private void runInteractiveGeneratorRestrictiveSettings(final String inputFile,
                                                            final int expectedTargetColumnCount,
                                                            final boolean hasHeaderRow) throws IOException {
        final var args = SchemaCliConfigTestUtility.builder()
                .input(inputFile)
                .output(schemaPath.toString())
                .subMode("--interactive")
                .noHeaders(!hasHeaderRow)
                .overwrite(true)
                .collaborationId(GeneralTestUtility.EXAMPLE_SALT.toString())
                .build();
        // number greater than test file column counts (test will fail if too low, so no incorrectness risk in
        // picking a number)
        final int columnCountUpperBound = 100;
        // user input which repeatedly says the source column in question should generate one cleartext column
        // with a trivial name
        final StringBuilder inputBuilder = new StringBuilder();
        for (int i = 0; i < columnCountUpperBound; i++) {
            // 1 target column
            inputBuilder.append("1\n");
            // target column type, will fail due to restrictive settings
            inputBuilder.append("cleartext\n");
            // target column type, will succeed
            inputBuilder.append("fingerprint\n");
            // target column name
            inputBuilder.append("column").append(i).append('\n');
            // skip suffix
            inputBuilder.append("\n");
        }
        final var userInput = new ByteArrayInputStream(inputBuilder.toString().getBytes(StandardCharsets.UTF_8));
        System.setIn(new BufferedInputStream(userInput));

        final var cleanRoomsDao = CleanRoomsDaoTestUtility.generateMockDao();
        when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(ClientSettings.highAssuranceMode());

        assertEquals(0, SchemaMode.getApp(cleanRoomsDao).execute(args.toArrayWithoutMode()));

        assertTrue(schemaPath.toFile().exists());
        assertTrue(schemaPath.toFile().length() > 0);
        final String contents = Files.readString(schemaPath);
        assertTrue(contents.contains("\"headerRow\": " + hasHeaderRow));
        assertEquals(hasHeaderRow ? expectedTargetColumnCount : 0,
                StringTestUtility.countMatches("sourceHeader", contents));
        assertEquals(expectedTargetColumnCount,
                StringTestUtility.countMatches("targetHeader", contents));
        assertEquals(0,
                StringTestUtility.countMatches(ColumnType.CLEARTEXT.toString(), contents));
        assertEquals(expectedTargetColumnCount,
                StringTestUtility.countMatches("\"" + ColumnType.FINGERPRINT + "\"", contents));
    }

    @Test
    public void schemaTemplateCsvTest() throws IOException {
        runTemplateGeneratorNoSettings("../samples/csv/data_sample_without_quotes.csv", true);
    }

    @Test
    public void schemaTemplateCsvNoHeadersTest() throws IOException {
        runTemplateGeneratorNoSettings("../samples/csv/data_sample_no_headers.csv", false);
    }

    @Test
    public void schemaTemplateWithPermissiveSettingsCsvTest() throws IOException {
        runTemplateGeneratorPermissiveSettings("../samples/csv/data_sample_without_quotes.csv", true);
    }

    @Test
    public void schemaTemplateWithPermissiveSettingsCsvNoHeadersTest() throws IOException {
        runTemplateGeneratorPermissiveSettings("../samples/csv/data_sample_no_headers.csv", false);
    }

    @Test
    public void schemaTemplateWithRestrictiveSettingsCsvTest() throws IOException {
        runTemplateGeneratorRestrictiveSettings("../samples/csv/data_sample_without_quotes.csv", SAMPLE_DATA_COLUMN_COUNT, true);
    }

    @Test
    public void schemaTemplateWithRestrictiveSettingsCsvNoHeadersTest() throws IOException {
        runTemplateGeneratorRestrictiveSettings("../samples/csv/data_sample_no_headers.csv", SAMPLE_DATA_COLUMN_COUNT, false);
    }

    @Test
    public void schemaTemplateParquetTest() throws IOException {
        runTemplateGeneratorNoSettings("../samples/parquet/data_sample.parquet", true);
    }

    @Test
    public void schemaTemplateWithPermissiveSettingsParquetTest() throws IOException {
        runTemplateGeneratorPermissiveSettings("../samples/parquet/data_sample.parquet", true);
    }

    @Test
    public void schemaTemplateWithRestrictiveSettingsParquetTest() throws IOException {
        runTemplateGeneratorRestrictiveSettings("../samples/parquet/data_sample.parquet", SAMPLE_DATA_COLUMN_COUNT, true);
    }

    @Test
    public void schemaTemplateWithRestrictiveSettingsParquetMixedDataTest() throws IOException {
        // only 8 columns are supported types, so we only expect 8 target columns
        runTemplateGeneratorRestrictiveSettings("../samples/parquet/rows_100_groups_10_prim_data.parquet", 8, true);
    }

    @Test
    public void schemaInteractiveCsvTest() throws IOException {
        runInteractiveGeneratorNoSettings("../samples/csv/data_sample_without_quotes.csv", true);
    }

    // Check that interactive schema command returns results and shallowly check content contains expected entries
    @Test
    public void schemaInteractiveCsvNoHeadersTest() throws IOException {
        runInteractiveGeneratorNoSettings("../samples/csv/data_sample_no_headers.csv", false);
    }

    @Test
    public void schemaInteractiveParquetTest() throws IOException {
        runInteractiveGeneratorNoSettings("../samples/parquet/data_sample.parquet", true);
    }

    @Test
    public void schemaInteractivePermissiveSettingsCsvTest() throws IOException {
        runInteractiveGeneratorPermissiveSettings("../samples/csv/data_sample_without_quotes.csv", true);
    }

    @Test
    public void schemaInteractivePermissiveSettingsCsvNoHeadersTest() throws IOException {
        runInteractiveGeneratorPermissiveSettings("../samples/csv/data_sample_no_headers.csv", false);
    }

    @Test
    public void schemaInteractivePermissiveSettingsParquetTest() throws IOException {
        runInteractiveGeneratorNoSettings("../samples/parquet/data_sample.parquet", true);
    }

    @Test
    public void schemaInteractiveRestrictiveSettingsCsvTest() throws IOException {
        runInteractiveGeneratorRestrictiveSettings("../samples/csv/data_sample_without_quotes.csv", SAMPLE_DATA_COLUMN_COUNT, true);
    }

    @Test
    public void schemaInteractiveRestrictiveSettingsCsvNoHeadersTest() throws IOException {
        runInteractiveGeneratorRestrictiveSettings("../samples/csv/data_sample_no_headers.csv", SAMPLE_DATA_COLUMN_COUNT, false);
    }

    @Test
    public void schemaInteractiveRestrictiveSettingsParquetTest() throws IOException {
        runInteractiveGeneratorRestrictiveSettings("../samples/parquet/data_sample.parquet", SAMPLE_DATA_COLUMN_COUNT, true);
    }

    @Test
    public void schemaInteractiveRestrictiveSettingsParquetMixedDataTest() throws IOException {
        // Only 8 columns are supported types, so we expect 8 target columns only
        runInteractiveGeneratorRestrictiveSettings("../samples/parquet/rows_100_groups_10_prim_data.parquet", 8, true);
    }
}
