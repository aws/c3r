// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cli;

import com.amazonaws.c3r.cleanrooms.CleanRoomsDao;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MainEnvVarKeyValidTest {
    private static final String ENC_INPUT_PATH = "../samples/csv/data_sample_without_quotes.csv";

    private static final String SCHEMA_PATH = "../samples/schema/config_sample.json";

    private static final String DEC_INPUT_PATH = "../samples/csv/marshalled_data_sample.csv";

    private DecryptCliConfigTestUtility decArgs;

    private CommandLine decMain;

    private EncryptCliConfigTestUtility encArgs;

    private CommandLine encMain;

    public int runEncryptMainWithCliArgs() {
        return encMain.execute(encArgs.toArrayWithoutMode());
    }

    public int runDecryptMainWithCliArgs() {
        return decMain.execute(decArgs.toArrayWithoutMode());
    }

    @BeforeEach
    public void setup() throws IOException {
        final Path tempDir = Files.createTempDirectory("temp");
        tempDir.toFile().deleteOnExit();
        final Path outputFile = Files.createTempFile(tempDir, "output", ".csv");
        outputFile.toFile().deleteOnExit();
        final String output = outputFile.toFile().getAbsolutePath();
        encArgs = EncryptCliConfigTestUtility.defaultDryRunTestArgs(ENC_INPUT_PATH, SCHEMA_PATH);
        encArgs.setOutput(output);
        final CleanRoomsDao cleanRoomsDao = mock(CleanRoomsDao.class);
        when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(encArgs.getClientSettings());
        encMain = EncryptMode.getApp(cleanRoomsDao);
        decArgs = DecryptCliConfigTestUtility.defaultDryRunTestArgs(DEC_INPUT_PATH);
        decArgs.setOutput(output);
        decMain = DecryptMode.getApp();
    }

    @Test
    public void validateEncryptSecretKeyInvalidTest() {
        assertEquals(0, runEncryptMainWithCliArgs());
    }

    @Test
    public void validateDecryptSecretKeyInvalidTest() {
        assertEquals(0, runDecryptMainWithCliArgs());
    }
}
