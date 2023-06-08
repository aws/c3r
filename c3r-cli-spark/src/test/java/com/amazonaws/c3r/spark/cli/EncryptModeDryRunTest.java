// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.cli;

import com.amazonaws.c3r.cleanrooms.CleanRoomsDao;
import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.spark.cleanrooms.CleanRoomsDaoTestUtility;
import com.amazonaws.c3r.spark.io.CsvTestUtility;
import com.amazonaws.c3r.spark.utils.FileTestUtility;
import com.amazonaws.c3r.spark.utils.GeneralTestUtility;
import com.amazonaws.c3r.spark.utils.SparkSessionTestUtility;
import com.amazonaws.c3r.utils.FileUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class EncryptModeDryRunTest {
    private static final String INPUT_PATH = "../samples/csv/data_sample_without_quotes.csv";

    private static final String SCHEMA_PATH = "../samples/schema/config_sample.json";

    private EncryptCliConfigTestUtility encArgs;

    private EncryptMode main;

    private CleanRoomsDao mockCleanRoomsDao;

    @BeforeEach
    public void setup() throws IOException {
        final String output = FileTestUtility.createTempDir().toString();
        encArgs = EncryptCliConfigTestUtility.defaultDryRunTestArgs(INPUT_PATH, SCHEMA_PATH);
        encArgs.setOutput(output);
        mockCleanRoomsDao = CleanRoomsDaoTestUtility.generateMockDao();
        when(mockCleanRoomsDao.getCollaborationDataEncryptionMetadata(any()))
                .thenAnswer((Answer<ClientSettings>) invocation -> encArgs.getClientSettings());
        main = new EncryptMode(mockCleanRoomsDao, SparkSessionTestUtility.initSparkSession());
    }

    public int runMainWithCliArgs() {
        return new CommandLine(main).execute(encArgs.toArrayWithoutMode());
    }

    @Test
    public void minimumViableArgsTest() {
        assertEquals(0, runMainWithCliArgs());
        assertEquals(SCHEMA_PATH, main.getRequiredArgs().getSchema());
        assertEquals(INPUT_PATH, main.getRequiredArgs().getInput());
        assertEquals(GeneralTestUtility.EXAMPLE_SALT, main.getRequiredArgs().getId());
    }

    @Test
    public void validateInputBlankTest() {
        encArgs.setInput("");
        assertNotEquals(0, runMainWithCliArgs());
    }

    @Test
    public void validateConfigBlankTest() {
        encArgs.setSchema("");
        assertNotEquals(0, runMainWithCliArgs());
    }

    @Test
    public void validateCollaborationIdBlankTest() {
        encArgs.setCollaborationId("");
        assertNotEquals(0, runMainWithCliArgs());
    }

    @Test
    public void validateCollaborationIdInvalidUuidTest() {
        encArgs.setCollaborationId("123456");
        assertNotEquals(0, runMainWithCliArgs());
    }

    @Test
    public void getTargetFileEmptyTest() {
        encArgs.setOutput("");
        assertNotEquals(0, runMainWithCliArgs());
    }

    private void checkBooleans(final Function<Boolean, Boolean> action) {
        assertEquals(true, action.apply(true));
        assertEquals(false, action.apply(false));
    }

    @Test
    public void allowCleartextFlagTest() {
        checkBooleans(b -> {
            encArgs.setAllowCleartext(b);
            runMainWithCliArgs();
            return main.getClientSettings().isAllowCleartext();
        });
    }

    @Test
    public void allowDuplicatesFlagTest() {
        checkBooleans(b -> {
            encArgs.setAllowDuplicates(b);
            runMainWithCliArgs();
            return main.getClientSettings().isAllowDuplicates();
        });
    }

    @Test
    public void allowJoinsOnColumnsWithDifferentNamesFlagTest() {
        checkBooleans(b -> {
            encArgs.setAllowJoinsOnColumnsWithDifferentNames(b);
            runMainWithCliArgs();
            return main.getClientSettings().isAllowJoinsOnColumnsWithDifferentNames();
        });

    }

    @Test
    public void preserveNullsFlagTest() {
        checkBooleans(b -> {
            encArgs.setPreserveNulls(b);
            runMainWithCliArgs();
            return main.getClientSettings().isPreserveNulls();
        });
    }

    @Test
    public void inputFileFormatTest() throws IOException {
        final String input = FileTestUtility.createTempFile("input", ".unknown").toString();

        encArgs.setInput(input);
        assertNotEquals(0, runMainWithCliArgs());
        encArgs.setFileFormat(FileFormat.CSV);
        assertEquals(0, runMainWithCliArgs());
    }

    @Test
    public void noProfileOrRegionFlagsTest() {
        main = new EncryptMode(mockCleanRoomsDao, SparkSessionTestUtility.initSparkSession());
        new CommandLine(main).execute(encArgs.toArrayWithoutMode());
        assertNull(main.getOptionalArgs().getProfile());
        assertNull(mockCleanRoomsDao.getRegion());
    }

    @Test
    public void profileFlagTest() throws IOException {
        // Ensure that passing a value via the --profile flag is given to the CleanRoomsDao builder's `profile(..)` method.
        final String myProfileName = "my-profile-name";
        assertNotEquals(myProfileName, mockCleanRoomsDao.toString());

        when(mockCleanRoomsDao.withRegion(any())).thenThrow(new RuntimeException("test failure - region should have have been set"));
        encArgs.setProfile(myProfileName);
        main = new EncryptMode(mockCleanRoomsDao, SparkSessionTestUtility.initSparkSession());
        new CommandLine(main).execute(encArgs.toArrayWithoutMode());
        assertEquals(myProfileName, main.getOptionalArgs().getProfile());
        assertEquals(myProfileName, main.getCleanRoomsDao().getProfile());
    }

    @Test
    public void regionFlagTest() {
        final String myRegion = "collywobbles";
        encArgs.setRegion(myRegion);
        main = new EncryptMode(mockCleanRoomsDao, SparkSessionTestUtility.initSparkSession());
        new CommandLine(main).execute(encArgs.toArrayWithoutMode());
        assertEquals(myRegion, main.getOptionalArgs().getRegion());
        assertEquals(myRegion, main.getCleanRoomsDao().getRegion());
    }

    /*
     * Add an extra column to a known valid schema and make sure it's not accepted because it doesn't have the same number
     * of columns as the csv file. Easiest to run through the CLI since we need the CSV parser for verification.
     */
    @Test
    public void tooManyColumnsPositionalSchemaTest() throws IOException {
        final String tempJson = FileUtil.readBytes("../samples/schema/config_sample_no_headers.json");
        final int closeOuter = tempJson.lastIndexOf("]");
        final String json = tempJson.substring(0, closeOuter - 1) + ", [] ] }";
        final Path schema = FileTestUtility.createTempFile("schema", ".json");
        Files.writeString(schema, json);

        final EncryptCliConfigTestUtility args =
                EncryptCliConfigTestUtility.defaultDryRunTestArgs("../samples/csv/data_sample_without_quotes.csv",
                        schema.toString());
        args.setDryRun(false);

        final var inputArgs = args.toArrayWithoutMode();
        assertEquals(Main.FAILURE, EncryptMode.getApp(null, SparkSessionTestUtility.initSparkSession()).execute(inputArgs));
    }

    /*
     * Remove a column to a known valid schema and make sure it's not accepted because it doesn't have the same number
     * of columns as the csv file. Easiest to run through the CLI since we need the CSV parser for verification.
     */
    @Test
    public void tooFewColumnsPositionalSchemaTest() throws IOException {
        final String tempJson = FileUtil.readBytes("../samples/schema/config_sample_no_headers.json");
        final int lastElementStart = tempJson.lastIndexOf("],");
        final String json = tempJson.substring(0, lastElementStart - 1) + "]]}";
        final Path schema = FileTestUtility.createTempFile("schema", ".json");
        Files.writeString(schema, json);

        final var args = EncryptCliConfigTestUtility.defaultDryRunTestArgs("../samples/csv/data_sample_no_headers" +
                ".csv", schema.toString());
        args.setDryRun(false);

        final var inputArgs = args.toArrayWithoutMode();
        assertEquals(Main.FAILURE, EncryptMode.getApp(null, SparkSessionTestUtility.initSparkSession()).execute(inputArgs));
    }

    /*
     * Make sure only the columns with ColumnSchemas are included in the output. Easiest to run through the CLI since we need
     * the CSV parser for verification.
     */
    @Test
    public void notAllColumnsUsedTest() throws IOException {
        final String json = "{ \"headerRow\": false, \"columns\": [" +
                "[{\"targetHeader\":\"firstname\", \"type\": \"cleartext\"}]," +
                "[]," +
                "[]," +
                "[]," +
                "[]," +
                "[]," +
                "[]," +
                "[]," +
                "[]" +
                "]}";
        final Path schema = FileTestUtility.createTempFile("schema", ".json");
        Files.writeString(schema, json);

        final EncryptCliConfigTestUtility args =
                EncryptCliConfigTestUtility.defaultDryRunTestArgs("../samples/csv/data_sample_without_quotes.csv", schema.toString());
        final String output = FileTestUtility.createTempDir().toString();
        args.setOutput(output);
        args.setDryRun(false);

        assertEquals(Main.SUCCESS, CliTestUtility.runWithoutCleanRooms(args));

        final Path mergedOutput = CsvTestUtility.mergeOutput(Path.of(output));
        final List<Map<String, String>> rows = CsvTestUtility.readRows(mergedOutput.toString());
        assertTrue(rows.size() > 0);
        for (Map<String, String> row : rows) {
            assertEquals(1, row.size());
            assertTrue(row.containsKey("firstname"));
        }
    }

}