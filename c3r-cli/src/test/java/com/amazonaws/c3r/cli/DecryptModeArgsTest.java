// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cli;

import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.utils.FileTestUtility;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class DecryptModeArgsTest {
    private static final String INPUT_PATH = "../samples/csv/marshalled_data_sample.csv";

    private DecryptCliConfigTestUtility decArgs;

    private DecryptMode main;

    @BeforeEach
    public void setup() throws IOException {
        final String output = FileTestUtility.createTempFile().toString();
        decArgs = DecryptCliConfigTestUtility.defaultDryRunTestArgs(INPUT_PATH);
        decArgs.setOutput(output);
        main = new DecryptMode();
    }

    public int runMainWithCliArgs() {
        return new CommandLine(main).execute(decArgs.toArrayWithoutMode());
    }

    @Test
    public void minimumViableArgsTest() {
        runMainWithCliArgs();
        assertEquals(INPUT_PATH, main.getRequiredArgs().getInput());
        assertEquals(GeneralTestUtility.EXAMPLE_SALT, main.getRequiredArgs().getId());
    }

    @Test
    public void missingRequiredDecryptArgFailsTest() {
        decArgs.setDryRun(false);
        decArgs.setEnableStackTraces(false);
        decArgs.setOverwrite(false);
        decArgs.setOutput(null);
        final var origArgs = decArgs.getCliArgsWithoutMode();

        for (int i = 0; i < origArgs.size(); i++) {
            final List<String> args = new ArrayList<>(origArgs);
            final String arg = origArgs.get(i);
            args.remove(arg);
            assertNotEquals(0, new CommandLine(main).execute(args.toArray(String[]::new)));
        }
    }

    @Test
    public void validateInputBlankTest() {
        decArgs.setInput("");
        assertNotEquals(0, runMainWithCliArgs());
    }

    @Test
    public void validateCollaborationIdBlankTest() {
        decArgs.setCollaborationId("");
        assertNotEquals(0, runMainWithCliArgs());
    }

    @Test
    public void validateCollaborationIdInvalidUuidTest() {
        decArgs.setCollaborationId("123456");
        assertNotEquals(0, runMainWithCliArgs());
    }

    @Test
    public void getTargetFileEmptyTest() {
        decArgs.setOutput("");
        assertNotEquals(0, runMainWithCliArgs());
    }

    @Test
    public void inputFileFormatTest() throws IOException {
        final String input = FileTestUtility.createTempFile("input", ".unknown").toString();

        decArgs.setInput(input);
        assertNotEquals(0, runMainWithCliArgs());
        decArgs.setFileFormat(FileFormat.CSV);
        assertEquals(0, runMainWithCliArgs());
    }
}
