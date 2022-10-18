// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cli;

import com.amazonaws.c3r.cleanrooms.CleanRoomsDao;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import nl.altindag.log.LogCaptor;
import nl.altindag.log.model.LogEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MainErrorMessageTest {
    private Path tempDir;

    private final String config = "../samples/schema/config_sample.json";

    private final String input = "../samples/csv/data_sample_without_quotes.csv";

    private EncryptCliConfigTestUtility encArgs;

    private DecryptCliConfigTestUtility decArgs;

    private SchemaCliConfigTestUtility schemaCliTestConfig;

    @BeforeEach
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("temp");
        tempDir.toFile().deleteOnExit();
        final Path outputPath = Files.createTempFile(tempDir, "MainTestSchemaAllowsFileOverwrite", ".out");
        outputPath.toFile().deleteOnExit();
        final String output = outputPath.toFile().getAbsolutePath();
        encArgs = EncryptCliConfigTestUtility.defaultTestArgs();
        encArgs.setSchema(config);
        encArgs.setAllowDuplicates(true);
        encArgs.setInput(input);
        encArgs.setOutput(output);

        decArgs = DecryptCliConfigTestUtility.defaultTestArgs();
        decArgs.setFailOnFingerprintColumns(false);
        decArgs.setInput(input);
        decArgs.setOutput(output);

        schemaCliTestConfig = SchemaCliConfigTestUtility.builder().input(input).output(output).build();
    }

    private void encryptAndCheckErrorMessagePresent(final EncryptCliConfigTestUtility encArgs, final boolean enableStackTraces,
                                                    final String message, final Class<? extends Throwable> expectedException) {
        final CleanRoomsDao cleanRoomsDao = mock(CleanRoomsDao.class);
        when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(encArgs.getClientSettings());
        final CommandLine cmd = EncryptMode.getApp(cleanRoomsDao);
        runAndCheckErrorMessagePresent(cmd, encArgs.toArrayWithoutMode(), enableStackTraces, message, expectedException);
    }

    private void decryptAndCheckErrorMessagePresent(final DecryptCliConfigTestUtility decArgs, final boolean enableStackTraces,
                                                    final String message, final Class<? extends Throwable> expectedException) {
        final CommandLine cmd = DecryptMode.getApp();
        runAndCheckErrorMessagePresent(cmd, decArgs.toArrayWithoutMode(), enableStackTraces, message, expectedException);
    }

    private void schemaAndCheckErrorMessagePresent(final SchemaCliConfigTestUtility args, final boolean enableStackTraces,
                                                   final String message) {
        final CommandLine cmd = SchemaMode.getApp();
        runAndCheckErrorMessagePresent(cmd, args.toArrayWithoutMode(), enableStackTraces, message, C3rIllegalArgumentException.class);
    }

    private void runAndCheckErrorMessagePresent(final CommandLine cmd, final String[] args, final boolean enableStackTraces,
                                                final String message, final Class<? extends Throwable> expectedException) {
        final List<LogEvent> logEvents;
        try (LogCaptor logCaptor = LogCaptor.forName("ROOT")) {
            cmd.execute(args);
            logEvents = logCaptor.getLogEvents();
        }
        assertFalse(logEvents.isEmpty());
        final LogEvent errorEvent = logEvents.get(logEvents.size() - 1); // The last message is the error
        final String outputMessage = errorEvent.getFormattedMessage();

        // Validate presence when stack traces enabled
        if (enableStackTraces) {
            assertTrue(errorEvent.getThrowable().isPresent());
            assertEquals(expectedException, errorEvent.getThrowable().get().getClass());
            assertTrue(outputMessage.contains(message));
        } else {
            // Validate presence when stack traces disabled
            assertFalse(errorEvent.getThrowable().isPresent());
            assertTrue(outputMessage.contains(message));
        }
    }

    @Test
    public void encryptInputIllegalArgumentExceptionTest() {
        final Path missingInput = tempDir.resolve("missingEncryptInputIllegalArgument.csv");
        encArgs.setInput(missingInput.toAbsolutePath().toString());

        encArgs.setEnableStackTraces(true);
        encryptAndCheckErrorMessagePresent(encArgs, true, "File does not exist", C3rIllegalArgumentException.class);
        encArgs.setEnableStackTraces(false);
        encryptAndCheckErrorMessagePresent(encArgs, false, "File does not exist", C3rIllegalArgumentException.class);
    }

    @Test
    public void decryptInputIllegalArgumentExceptionTest() {
        final Path missingInput = tempDir.resolve("missingDecryptInputIllegalArgument.csv");
        decArgs.setInput(missingInput.toAbsolutePath().toString());

        decArgs.setEnableStackTraces(true);
        decryptAndCheckErrorMessagePresent(decArgs, true, "File does not exist", C3rIllegalArgumentException.class);
        decArgs.setEnableStackTraces(false);
        decryptAndCheckErrorMessagePresent(decArgs, false, "File does not exist", C3rIllegalArgumentException.class);
    }

    @Test
    public void schemaValidateIllegalArgumentExceptionTest() {
        final Path missingInput = tempDir.resolve("missingSchemaValidateIllegalArgument.csv");
        schemaCliTestConfig.setInput(missingInput.toAbsolutePath().toString());
        schemaCliTestConfig.setSubMode("--template");

        schemaCliTestConfig.setEnableStackTraces(true);
        schemaAndCheckErrorMessagePresent(schemaCliTestConfig, true, "File does not exist");
        schemaCliTestConfig.setEnableStackTraces(false);
        schemaAndCheckErrorMessagePresent(schemaCliTestConfig, false, "File does not exist");
    }

    @Test
    public void encryptDuplicatesRuntimeExceptionTest() {
        encArgs.setAllowDuplicates(false);

        encArgs.setEnableStackTraces(true);
        encryptAndCheckErrorMessagePresent(encArgs, true, "Duplicate entries found", C3rRuntimeException.class);
        encArgs.setEnableStackTraces(false);
        encryptAndCheckErrorMessagePresent(encArgs, false, "Duplicate entries found", C3rRuntimeException.class);
    }

    @Test
    public void encryptPadFailureRuntimeExceptionTest() throws IOException {
        final Path badSample = tempDir.resolve("bad_data_sample.csv");
        badSample.toFile().deleteOnExit();
        final Path sample = new File(input).toPath();
        Files.copy(sample, badSample, StandardCopyOption.REPLACE_EXISTING);
        final byte[] bits = new byte[150];
        Arrays.fill(bits, (byte) 'a');
        final String badValue = new String(bits, StandardCharsets.UTF_8);
        final String unpaddableRow = "Shana,Hendrix,8 Hollows Rd,Richmond,VA,407-555-4322," + badValue + ",5,Sean's older sister\n";
        Files.write(badSample, unpaddableRow.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        encArgs.setInput(badSample.toAbsolutePath().toString());

        encArgs.setEnableStackTraces(true);
        encryptAndCheckErrorMessagePresent(encArgs, true, "No room for padding", C3rRuntimeException.class);
        encArgs.setEnableStackTraces(false);
        encryptAndCheckErrorMessagePresent(encArgs, false, "No room for padding", C3rRuntimeException.class);
    }

    @Test
    public void decryptFailOnFingerprintColumnsUnsupportedOperationExceptionTest() {
        decArgs.setInput("../samples/csv/marshalled_data_sample.csv");
        decArgs.setFailOnFingerprintColumns(true);

        decArgs.setEnableStackTraces(true);
        decryptAndCheckErrorMessagePresent(decArgs, true,
                "Data encrypted for a fingerprint column was found but is forbidden with current settings.", C3rRuntimeException.class);
        decArgs.setEnableStackTraces(false);
        decryptAndCheckErrorMessagePresent(decArgs, false,
                "Data encrypted for a fingerprint column was found but is forbidden with current settings.", C3rRuntimeException.class);
    }

    @Test
    public void encryptOverwriteOutputWhenFileExistsTest() {
        encArgs.setOverwrite(false);

        encArgs.setEnableStackTraces(true);
        encryptAndCheckErrorMessagePresent(encArgs, true, "File already exists", C3rIllegalArgumentException.class);
        encArgs.setEnableStackTraces(false);
        encryptAndCheckErrorMessagePresent(encArgs, false, "File already exists", C3rIllegalArgumentException.class);
    }

    @Test
    public void decryptOverwriteOutputWhenFileExistsTest() {
        decArgs.setOverwrite(false);

        decArgs.setEnableStackTraces(true);
        decryptAndCheckErrorMessagePresent(decArgs, true, "File already exists", C3rIllegalArgumentException.class);
        decArgs.setEnableStackTraces(false);
        decryptAndCheckErrorMessagePresent(decArgs, false, "File already exists", C3rIllegalArgumentException.class);
    }

    @Test
    public void schemaOverwriteOutputWhenFileExistsTest() {
        schemaCliTestConfig.setSubMode("--interactive");
        schemaCliTestConfig.setOverwrite(false);

        schemaCliTestConfig.setEnableStackTraces(true);
        schemaAndCheckErrorMessagePresent(schemaCliTestConfig, true, "File already exists");
        schemaCliTestConfig.setEnableStackTraces(false);
        schemaAndCheckErrorMessagePresent(schemaCliTestConfig, false, "File already exists");
    }

    @Test
    public void encryptAllowCleartextFalseTest() {
        encArgs.setAllowCleartext(false);

        encArgs.setEnableStackTraces(true);
        encryptAndCheckErrorMessagePresent(encArgs, true, "Cleartext columns found", C3rIllegalArgumentException.class);
        encArgs.setEnableStackTraces(false);
        encryptAndCheckErrorMessagePresent(encArgs, false, "Cleartext columns found", C3rIllegalArgumentException.class);
    }

    @Test
    public void encryptUnrecognizedFileFormatTest() throws IOException {
        final Path unknownInputFormat = Files.createTempFile(tempDir, "unknownInputFormat", "unknown");
        unknownInputFormat.toFile().deleteOnExit();
        encArgs.setInput(unknownInputFormat.toFile().getAbsolutePath());

        encArgs.setEnableStackTraces(true);
        encryptAndCheckErrorMessagePresent(encArgs, true, "Unknown file extension", C3rIllegalArgumentException.class);
        encArgs.setEnableStackTraces(false);
        encryptAndCheckErrorMessagePresent(encArgs, false, "Unknown file extension", C3rIllegalArgumentException.class);
    }

    @Test
    public void decryptUnrecognizedFileFormatTest() throws IOException {
        final Path unknownInputFormat = Files.createTempFile(tempDir, "unknownInputFormat", "unknown");
        unknownInputFormat.toFile().deleteOnExit();
        decArgs.setInput(unknownInputFormat.toFile().getAbsolutePath());

        decArgs.setEnableStackTraces(true);
        decryptAndCheckErrorMessagePresent(decArgs, true, "Unknown file extension", C3rIllegalArgumentException.class);
        decArgs.setEnableStackTraces(false);
        decryptAndCheckErrorMessagePresent(decArgs, false, "Unknown file extension", C3rIllegalArgumentException.class);
    }

    @Test
    public void schemaUnrecognizedFileFormatTest() throws IOException {
        schemaCliTestConfig.setSubMode("--interactive");

        final Path unknownInputFormat = Files.createTempFile(tempDir, "unknownInputFormat", "unknown");
        unknownInputFormat.toFile().deleteOnExit();
        schemaCliTestConfig.setInput(unknownInputFormat.toFile().getAbsolutePath());

        schemaCliTestConfig.setEnableStackTraces(true);
        schemaAndCheckErrorMessagePresent(schemaCliTestConfig, true, "Unknown file format");
        schemaCliTestConfig.setEnableStackTraces(false);
        schemaAndCheckErrorMessagePresent(schemaCliTestConfig, false, "Unknown file format");
    }

    @Test
    public void encryptEmptySchemaTest() throws IOException {
        final Path emptySchema = Files.createTempFile(tempDir, "emptySchema", ".json");
        emptySchema.toFile().deleteOnExit();
        encArgs.setSchema(emptySchema.toAbsolutePath().toString());

        encArgs.setEnableStackTraces(true);
        encryptAndCheckErrorMessagePresent(encArgs, true, "The table schema file was empty", C3rIllegalArgumentException.class);
        encArgs.setEnableStackTraces(false);
        encryptAndCheckErrorMessagePresent(encArgs, false, "The table schema file was empty", C3rIllegalArgumentException.class);
    }
}
