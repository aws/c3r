// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.utils.FileTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.amazonaws.c3r.utils.GeneralTestUtility.TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DecryptConfigTest {
    private String output;

    private DecryptConfig.DecryptConfigBuilder minimalConfigBuilder(final String sourceFile) {
        return DecryptConfig.builder()
                .secretKey(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getKey())
                .sourceFile(sourceFile)
                .targetFile(output)
                .salt(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getSalt());
    }

    @BeforeEach
    public void setup() throws IOException {
        output = FileTestUtility.resolve("output.csv").toString();
    }

    @Test
    public void minimumViableConstructionTest() {
        assertDoesNotThrow(() -> minimalConfigBuilder(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getInput()).build());
    }

    @Test
    public void validateInputEmptyTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> minimalConfigBuilder(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getInput())
                .sourceFile("").build());
    }

    @Test
    public void validateOutputEmptyTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> minimalConfigBuilder(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getInput())
                .targetFile("").build());
    }

    @Test
    public void validateNoOverwriteTest() throws IOException {
        output = FileTestUtility.createTempFile().toString();
        assertThrows(C3rIllegalArgumentException.class, () -> minimalConfigBuilder(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getInput())
                .overwrite(false).build());
    }

    @Test
    public void validateOverwriteTest() throws IOException {
        output = FileTestUtility.createTempFile().toString();
        assertDoesNotThrow(() -> minimalConfigBuilder(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getInput())
                .overwrite(true).build());
    }

    @Test
    public void validateEmptySaltTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> minimalConfigBuilder(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getInput())
                .salt("").build());
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
    public void csvOptionsNonCsvFileFormatTest() throws IOException {
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
}
