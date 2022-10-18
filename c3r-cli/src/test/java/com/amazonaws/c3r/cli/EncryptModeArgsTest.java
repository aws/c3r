// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cli;

import com.amazonaws.c3r.cleanrooms.CleanRoomsDao;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EncryptModeArgsTest {
    private static final String INPUT_PATH = "../samples/csv/data_sample_without_quotes.csv";

    private static final String SCHEMA_PATH = "../samples/schema/config_sample.json";

    private EncryptCliConfigTestUtility encArgs;

    private EncryptMode main;

    private CleanRoomsDao cleanRoomsDao;

    @BeforeEach
    public void setup() {
        String output = null;
        try {
            final Path tempDir = Files.createTempDirectory("temp");
            tempDir.toFile().deleteOnExit();
            final Path outputFile = Files.createTempFile(tempDir, "output", ".csv");
            outputFile.toFile().deleteOnExit();
            output = outputFile.toFile().getAbsolutePath();
        } catch (IOException e) {
            fail("Could not setup temp dir for tests.");
        }
        encArgs = EncryptCliConfigTestUtility.defaultDryRunTestArgs(INPUT_PATH, SCHEMA_PATH);
        encArgs.setOutput(output);
        cleanRoomsDao = mock(CleanRoomsDao.class);
        when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(encArgs.getClientSettings());
        main = new EncryptMode(cleanRoomsDao);
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
    public void missingRequiredEncryptArgFailsTest() {
        encArgs.setDryRun(false);
        encArgs.setEnableStackTraces(false);
        encArgs.setOverwrite(false);
        encArgs.setOutput(null);
        final var origArgs = encArgs.getCliArgsWithoutMode();
        for (int i = 0; i < origArgs.size(); i++) {
            final List<String> args = new ArrayList<>(origArgs);
            final String arg = origArgs.get(i);
            args.remove(arg);
            assertNotEquals(0, new CommandLine(main).execute(args.toArray(String[]::new)));
        }
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
            setup();
            encArgs.setAllowCleartext(b);
            when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(encArgs.getClientSettings());
            runMainWithCliArgs();
            return main.getClientSettings().isAllowCleartext();
        });
    }

    @Test
    public void allowDuplicatesFlagTest() {
        checkBooleans(b -> {
            setup();
            encArgs.setAllowDuplicates(b);
            when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(encArgs.getClientSettings());
            runMainWithCliArgs();
            return main.getClientSettings().isAllowDuplicates();
        });
    }

    @Test
    public void allowJoinsOnColumnsWithDifferentNamesFlagTest() {
        checkBooleans(b -> {
            setup();
            encArgs.setAllowJoinsOnColumnsWithDifferentNames(b);
            when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(encArgs.getClientSettings());
            runMainWithCliArgs();
            return main.getClientSettings().isAllowJoinsOnColumnsWithDifferentNames();
        });

    }

    @Test
    public void preserveNullsFlagTest() {
        checkBooleans(b -> {
            setup();
            encArgs.setPreserveNulls(b);
            when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(encArgs.getClientSettings());
            runMainWithCliArgs();
            return main.getClientSettings().isPreserveNulls();
        });
    }

    @Test
    public void inputFileFormatTest() throws IOException {
        final Path input = Files.createTempFile("input", ".unknown");
        input.toFile().deleteOnExit();

        encArgs.setInput(input.toString());
        assertNotEquals(0, runMainWithCliArgs());
        encArgs.setFileFormat(FileFormat.CSV);
        assertEquals(0, runMainWithCliArgs());
    }
}