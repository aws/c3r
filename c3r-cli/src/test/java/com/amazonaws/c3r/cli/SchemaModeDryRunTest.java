// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cli;

import com.amazonaws.c3r.cleanrooms.CleanRoomsDao;
import com.amazonaws.c3r.cleanrooms.CleanRoomsDaoTestUtility;
import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.utils.FileTestUtility;
import com.amazonaws.c3r.utils.FileUtil;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import software.amazon.awssdk.regions.Region;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class SchemaModeDryRunTest {
    private static final String INPUT_CSV_PATH = "../samples/csv/data_sample_without_quotes.csv";

    private static final String INPUT_PARQUET_PATH = "../samples/parquet/data_sample.parquet";

    private SchemaCliConfigTestUtility schemaArgs;

    private SchemaMode main;

    private CleanRoomsDao mockCleanRoomsDao;

    @BeforeEach
    public void setup() throws IOException {
        final String output = FileTestUtility.createTempFile("schema", ".json").toString();
        schemaArgs = SchemaCliConfigTestUtility.builder().overwrite(true).input(INPUT_CSV_PATH)
                .output(output).build();
        mockCleanRoomsDao = CleanRoomsDaoTestUtility.generateMockDao();
        when(mockCleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(ClientSettings.lowAssuranceMode());
    }

    public void runMainWithCliArgs(final boolean passes) {
        main = new SchemaMode(mockCleanRoomsDao);
        final int exitCode = new CommandLine(main).execute(schemaArgs.toArrayWithoutMode());
        if (passes) {
            assertEquals(0, exitCode);
        } else {
            assertNotEquals(0, exitCode);
        }
    }

    @Test
    public void minimumViableArgsTest() {
        runMainWithCliArgs(true);
        assertEquals(INPUT_CSV_PATH, main.getRequiredArgs().getInput());
    }

    @Test
    public void defaultOutputFileTest() {
        final File sourceFile = new File(INPUT_CSV_PATH);
        final File targetFile = new File(sourceFile.getName() + ".json");
        targetFile.deleteOnExit();
        schemaArgs.setOutput(null);
        runMainWithCliArgs(true);
        assertNull(main.getOptionalArgs().getOutput());
        assertTrue(targetFile.exists());
        assertTrue(targetFile.length() > 0);
        // assert sourceFile directory is stripped and targetFile is associated with the working directory.
        assertNotNull(sourceFile.getParentFile());
        assertNull(targetFile.getParentFile());
        assertTrue(targetFile.getAbsolutePath().contains(FileUtil.CURRENT_DIR));
    }

    @Test
    public void specifiedOutputFileTest() {
        final File schemaOutput = new File("output.json");
        schemaOutput.deleteOnExit();
        schemaArgs.setOutput("output.json");
        runMainWithCliArgs(true);
        assertEquals("output.json", main.getOptionalArgs().getOutput());
    }

    @Test
    public void validateInputBlankTest() {
        schemaArgs.setInput("--invalid");
        runMainWithCliArgs(false);
    }

    @Test
    public void getTargetFileEmptyTest() {
        schemaArgs.setOutput("");
        runMainWithCliArgs(false);
    }

    @Test
    public void validateBadLogLevelErrorTest() {
        schemaArgs.setVerbosity("SUPER-LOUD-PLEASE");
        runMainWithCliArgs(false);
    }

    @Test
    public void schemaInteractiveTerminatedInputTest() throws IOException {
        final Path schemaPath = Files.createTempFile("schema", ".json");
        schemaPath.toFile().deleteOnExit();

        schemaArgs.setOutput(schemaPath.toAbsolutePath().toString());
        schemaArgs.setSubMode("--interactive");
        final var args = schemaArgs.toList();
        args.remove(0);

        // user input which ends unexpectedly during interactive CLI session
        final var userInput = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
        System.setIn(new BufferedInputStream(userInput));

        final int exitCode = new CommandLine(new SchemaMode(mockCleanRoomsDao)).execute(args.toArray(new String[0]));
        assertNotEquals(0, exitCode);

        assertTrue(schemaPath.toFile().exists());
        assertEquals(0, schemaPath.toFile().length());
    }

    @Test
    public void testInvalidModeSetting() {
        final ByteArrayOutputStream consoleOutput = new ByteArrayOutputStream();
        final PrintStream pErr = new PrintStream(consoleOutput);
        System.setErr(pErr);
        schemaArgs.setSubMode("--invalidMode");
        runMainWithCliArgs(false);
        final String expected = "Unknown option: '--invalidMode'";
        assertTrue(consoleOutput.toString(StandardCharsets.UTF_8).contains(expected));
    }

    @Test
    public void testMissingModeSettings() {
        final ByteArrayOutputStream nullConsoleOutput = new ByteArrayOutputStream();
        final PrintStream pNullErr = new PrintStream(nullConsoleOutput);
        System.setErr(pNullErr);
        assertDoesNotThrow(() -> new CommandLine(new SchemaMode(mockCleanRoomsDao))
                .execute("--output=" + schemaArgs.getOutput(), INPUT_CSV_PATH));
        assertTrue(nullConsoleOutput.toString(StandardCharsets.UTF_8)
                .startsWith("Error: Missing required argument (specify one of these):"
                + " (-t | -i)"));
    }

    @Test
    public void unknownFileFormatTest() throws IOException {
        final String schemaUnknownExtensionPath = FileTestUtility.createTempFile("schema", ".unknown").toString();
        schemaArgs.setInput(schemaUnknownExtensionPath);
        schemaArgs.setFileFormat(null);
        runMainWithCliArgs(false);
    }

    @Test
    public void supportedFileFormatFlagCsvTest() {
        schemaArgs.setInput(INPUT_CSV_PATH);
        schemaArgs.setFileFormat(FileFormat.CSV);
        runMainWithCliArgs(true);
    }

    @Test
    public void unsupportedFileFormatFlagTest() throws IOException {
        final String schemaUnsupportedExtensionPath = FileTestUtility.createTempFile("schema", ".unsupported").toString();
        schemaArgs.setInput(schemaUnsupportedExtensionPath);
        schemaArgs.setFileFormat(FileFormat.PARQUET);
        runMainWithCliArgs(false);
    }

    @Test
    public void supportedFileFormatFlagParquetTest() {
        schemaArgs.setInput(INPUT_PARQUET_PATH);
        schemaArgs.setFileFormat(FileFormat.PARQUET);
        runMainWithCliArgs(true);
    }

    @Test
    public void noHeadersCsvTest() {
        schemaArgs.setInput(INPUT_CSV_PATH);
        schemaArgs.setFileFormat(FileFormat.CSV);
        schemaArgs.setNoHeaders(true);
        runMainWithCliArgs(true);
    }

    @Test
    public void noHeadersParquetTest() {
        schemaArgs.setInput(INPUT_PARQUET_PATH);
        schemaArgs.setFileFormat(FileFormat.PARQUET);
        schemaArgs.setNoHeaders(true);
        runMainWithCliArgs(false);
    }

    @Test
    public void testInvalidIdFormat() {
        schemaArgs.setInput(INPUT_CSV_PATH);
        schemaArgs.setCollaborationId("invalidCollaborationId");
        runMainWithCliArgs(false);
    }

    @Test
    public void testValidId() {
        schemaArgs.setInput(INPUT_CSV_PATH);
        schemaArgs.setCollaborationId(GeneralTestUtility.EXAMPLE_SALT.toString());
        runMainWithCliArgs(true);
    }

    @Test
    public void noProfileOrRegionFlagsTest() {
        // Ensure that if no profile or region flag are passed, then the CleanRoomsDao are not constructed
        // with any explicit values for them (i.e., ensuring the defaults are used)
        main = new SchemaMode(mockCleanRoomsDao);
        new CommandLine(main).execute(schemaArgs.toArrayWithoutMode());
        assertNull(main.getOptionalArgs().getProfile());
        assertNull(main.getCleanRoomsDao().getRegion());
    }

    @Test
    public void profileFlagTest() throws IOException {
        // Ensure that passing a value via the --profile flag is given to the CleanRoomsDao builder's `profile(..)` method.
        final String myProfileName = "my-profile-name";
        assertNotEquals(myProfileName, mockCleanRoomsDao.toString());

        schemaArgs.setProfile(myProfileName);
        schemaArgs.setCollaborationId(UUID.randomUUID().toString());
        main = new SchemaMode(mockCleanRoomsDao);
        new CommandLine(main).execute(schemaArgs.toArrayWithoutMode());
        assertEquals(myProfileName, main.getOptionalArgs().getProfile());
        assertEquals(myProfileName, main.getCleanRoomsDao().getProfile());
    }

    @Test
    public void regionFlagTest() {
        final String myRegion = "collywobbles";
        schemaArgs.setRegion(myRegion);
        schemaArgs.setCollaborationId(UUID.randomUUID().toString());
        main = new SchemaMode(mockCleanRoomsDao);
        new CommandLine(main).execute(schemaArgs.toArrayWithoutMode());
        assertEquals(myRegion, main.getOptionalArgs().getRegion());
        assertEquals(Region.of(myRegion), main.getCleanRoomsDao().getRegion());
    }
}
