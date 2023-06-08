// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.config;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.spark.utils.FileTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.amazonaws.c3r.spark.utils.GeneralTestUtility.TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

public class SparkDecryptConfigTest {
    private String output;

    private SparkDecryptConfig.SparkDecryptConfigBuilder minimalConfigBuilder(final String sourceFile) {
        return SparkDecryptConfig.builder()
                .secretKey(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getKey())
                .source(sourceFile)
                .targetDir(output)
                .salt(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getSalt());
    }

    @BeforeEach
    public void setup() throws IOException {
        output = FileTestUtility.createTempDir().resolve("outputDir").toString();
    }

    @Test
    public void minimumViableConstructionTest() {
        assertDoesNotThrow(() -> minimalConfigBuilder(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getInput()).build());
    }

    @Test
    public void validateInputEmptyTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> minimalConfigBuilder(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getInput())
                .source("").build());
    }

    @Test
    public void validateOutputEmptyTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> minimalConfigBuilder(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getInput())
                .targetDir("").build());
    }

    @Test
    public void validateNoOverwriteTest() throws IOException {
        output = FileTestUtility.createTempFile().toString();
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
        assertThrows(C3rIllegalArgumentException.class, () ->
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
        assertThrows(C3rIllegalArgumentException.class, () ->
                minimalConfigBuilder(parquetPath)
                        .csvInputNullValue("")
                        .build());

        // parquet file with csvOutputNullValue errors
        assertThrows(C3rIllegalArgumentException.class, () ->
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
}
