// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.cli;

import com.amazonaws.c3r.spark.utils.SparkSessionTestUtility;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

/*
 * Tests specifically needing an invalid key in the environment
 * variable for the shared secret key.
 */
public class MainEnvVarKeyInvalidTest {
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
    public void setup() {
        final SparkSession sparkSession = SparkSessionTestUtility.initSparkSession();
        encArgs = EncryptCliConfigTestUtility.defaultDryRunTestArgs(ENC_INPUT_PATH, SCHEMA_PATH);
        encMain = EncryptMode.getApp(null, sparkSession);
        decArgs = DecryptCliConfigTestUtility.defaultDryRunTestArgs(DEC_INPUT_PATH);
        decMain = DecryptMode.getApp(sparkSession);
    }

    @Test
    public void validateEncryptSecretKeyInvalidTest() {
        assertNotEquals(0, runEncryptMainWithCliArgs());
    }

    @Test
    public void validateDecryptSecretKeyInvalidTest() {
        assertNotEquals(0, runDecryptMainWithCliArgs());
    }
}
