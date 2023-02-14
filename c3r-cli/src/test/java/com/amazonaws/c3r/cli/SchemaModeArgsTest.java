// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cli;

import com.amazonaws.c3r.cleanrooms.CleanRoomsDao;
import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.utils.FileTestUtility;
import com.amazonaws.c3r.utils.FileUtil;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemaModeArgsTest {
    private static final String INPUT_CSV_PATH = "../samples/csv/data_sample_without_quotes.csv";

    private static final String INPUT_PARQUET_PATH = "../samples/parquet/data_sample.parquet";

    private SchemaCliConfigTestUtility schemaCliTestConfig;

    private SchemaMode main;

    private CleanRoomsDao cleanRoomsDao;

    @BeforeEach
    public void setup() throws IOException {
        final String output = FileTestUtility.createTempFile("schema", ".json").toString();
        schemaCliTestConfig = SchemaCliConfigTestUtility.builder().overwrite(true).input(INPUT_CSV_PATH)
                .output(output).build();
        cleanRoomsDao = mock(CleanRoomsDao.class);
        when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(ClientSettings.lowAssuranceMode());
        main = new SchemaMode(cleanRoomsDao);
    }

    public void runMainWithCliArgs(final boolean passes) {
        final var args = schemaCliTestConfig.toListWithoutMode();
        if (passes) {
            assertEquals(0, new CommandLine(main).execute(args.toArray(new String[0])));
        } else {
            assertNotEquals(0, new CommandLine(main).execute(args.toArray(new String[0])));
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
        schemaCliTestConfig.setOutput(null);
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
        schemaCliTestConfig.setOutput("output.json");
        runMainWithCliArgs(true);
        assertEquals("output.json", main.getOptionalArgs().getOutput());
    }

    @Test
    public void missingRequiredSchemaModeArgFailsTest() {
        schemaCliTestConfig.setOutput(null);
        schemaCliTestConfig.setOverwrite(false);
        schemaCliTestConfig.setEnableStackTraces(false);
        final var origArgs = schemaCliTestConfig.toListWithoutMode();
        for (int i = 0; i < origArgs.size(); i++) {
            final List<String> args = new ArrayList<>(origArgs);
            final String arg = origArgs.get(i);
            args.remove(arg);
            final int exitCode = new CommandLine(main).execute(args.toArray(String[]::new));
            assertNotEquals(0, exitCode);
        }
    }

    @Test
    public void validateInputBlankTest() {
        schemaCliTestConfig.setInput("--invalid");
        runMainWithCliArgs(false);
    }

    @Test
    public void getTargetFileEmptyTest() {
        schemaCliTestConfig.setOutput("");
        runMainWithCliArgs(false);
    }

    @Test
    public void validateBadLogLevelErrorTest() {
        schemaCliTestConfig.setVerbosity("SUPER-LOUD-PLEASE");
        runMainWithCliArgs(false);
    }

    @Test
    public void schemaInteractiveTerminatedInputTest() throws IOException {
        final Path schemaPath = Files.createTempFile("schema", ".json");
        schemaPath.toFile().deleteOnExit();

        schemaCliTestConfig.setOutput(schemaPath.toAbsolutePath().toString());
        schemaCliTestConfig.setSubMode("--interactive");
        final var args = schemaCliTestConfig.toList();
        args.remove(0);

        // user input which ends unexpectedly during interactive CLI session
        final var userInput = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
        System.setIn(new BufferedInputStream(userInput));

        final int exitCode = new CommandLine(main).execute(args.toArray(new String[0]));
        assertNotEquals(0, exitCode);

        assertTrue(schemaPath.toFile().exists());
        assertEquals(0, schemaPath.toFile().length());
    }

    @Test
    public void testInvalidModeSetting() {
        final ByteArrayOutputStream consoleOutput = new ByteArrayOutputStream();
        final PrintStream pErr = new PrintStream(consoleOutput);
        System.setErr(pErr);
        schemaCliTestConfig.setSubMode("--invalidMode");
        runMainWithCliArgs(false);
        final String expected = "Unknown option: '--invalidMode'";
        assertTrue(consoleOutput.toString(StandardCharsets.UTF_8).contains(expected));
    }

    @Test
    public void testMissingModeSettings() {
        final ByteArrayOutputStream nullConsoleOutput = new ByteArrayOutputStream();
        final PrintStream pNullErr = new PrintStream(nullConsoleOutput);
        System.setErr(pNullErr);
        assertDoesNotThrow(() -> new CommandLine(main).execute("--output=" + schemaCliTestConfig.getOutput(), INPUT_CSV_PATH));
        assertTrue(nullConsoleOutput.toString(StandardCharsets.UTF_8).startsWith("Error: Missing required argument (specify one of these):"
                + " (-t | -i)"));
    }

    @Test
    public void unknownFileFormatTest() throws IOException {
        final String schemaUnknownExtensionPath = FileTestUtility.createTempFile("schema", ".unknown").toString();
        schemaCliTestConfig.setInput(schemaUnknownExtensionPath);
        schemaCliTestConfig.setFileFormat(null);
        runMainWithCliArgs(false);
    }

    @Test
    public void supportedFileFormatFlagCsvTest() {
        schemaCliTestConfig.setInput(INPUT_CSV_PATH);
        schemaCliTestConfig.setFileFormat(FileFormat.CSV);
        runMainWithCliArgs(true);
    }

    @Test
    public void unsupportedFileFormatFlagTest() throws IOException {
        final String schemaUnsupportedExtensionPath = FileTestUtility.createTempFile("schema", ".unsupported").toString();
        schemaCliTestConfig.setInput(schemaUnsupportedExtensionPath);
        schemaCliTestConfig.setFileFormat(FileFormat.PARQUET);
        runMainWithCliArgs(false);
    }

    @Test
    public void supportedFileFormatFlagParquetTest() {
        schemaCliTestConfig.setInput(INPUT_PARQUET_PATH);
        schemaCliTestConfig.setFileFormat(FileFormat.PARQUET);
        runMainWithCliArgs(true);
    }

    @Test
    public void noHeadersCsvTest() {
        schemaCliTestConfig.setInput(INPUT_CSV_PATH);
        schemaCliTestConfig.setFileFormat(FileFormat.CSV);
        schemaCliTestConfig.setNoHeaders(true);
        runMainWithCliArgs(true);
    }

    @Test
    public void noHeadersParquetTest() {
        schemaCliTestConfig.setInput(INPUT_PARQUET_PATH);
        schemaCliTestConfig.setFileFormat(FileFormat.PARQUET);
        schemaCliTestConfig.setNoHeaders(true);
        runMainWithCliArgs(false);
    }

    @Test
    public void testInvalidIdFormat() {
        schemaCliTestConfig.setInput(INPUT_CSV_PATH);
        schemaCliTestConfig.setCollaborationId("invalidCollaborationId");
        runMainWithCliArgs(false);
    }

    @Test
    public void testValidId() {
        schemaCliTestConfig.setInput(INPUT_CSV_PATH);
        schemaCliTestConfig.setCollaborationId(GeneralTestUtility.EXAMPLE_SALT.toString());
        runMainWithCliArgs(true);
    }
}
