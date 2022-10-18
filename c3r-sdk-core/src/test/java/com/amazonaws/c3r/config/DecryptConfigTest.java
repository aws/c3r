// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.amazonaws.c3r.utils.GeneralTestUtility.TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DecryptConfigTest {
    private Path tempDir;

    private Path outputFile;

    private DecryptConfig.DecryptConfigBuilder minimalConfigBuilder(final String sourceFile) {
        return DecryptConfig.builder()
                .secretKey(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getKey())
                .sourceFile(sourceFile)
                .targetFile(outputFile.toFile().getAbsolutePath())
                .salt(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getSalt());
    }

    @BeforeEach
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("temp");
        tempDir.toFile().deleteOnExit();
        outputFile = tempDir.resolve("output.csv");
        outputFile.toFile().deleteOnExit();
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
        Files.createFile(outputFile);
        assertThrows(C3rIllegalArgumentException.class, () -> minimalConfigBuilder(TEST_CONFIG_ONE_ROW_NULL_SAMPLE_CLEARTEXT.getInput())
                .overwrite(false).build());
    }

    @Test
    public void validateOverwriteTest() throws IOException {
        Files.createFile(outputFile);
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
        final Path pathWithUnknownExtension = Files.createTempFile("input", ".unknown");
        pathWithUnknownExtension.toFile().deleteOnExit();
        // unknown extensions cause failure if no FileFormat is specified
        assertThrows(C3rIllegalArgumentException.class, () ->
                minimalConfigBuilder(pathWithUnknownExtension.toString()).build());

        // specifying a FileFormat makes it work
        assertDoesNotThrow(() ->
                minimalConfigBuilder(pathWithUnknownExtension.toString())
                        .fileFormat(FileFormat.CSV)
                        .build());
    }

    @Test
    public void csvOptionsNonCsvFileFormatTest() throws IOException {
        final Path parquetPath = Files.createTempFile("input", ".parquet");
        parquetPath.toFile().deleteOnExit();
        // parquet file is fine
        assertDoesNotThrow(() ->
                minimalConfigBuilder(parquetPath.toString()).build());

        // parquet file with csvInputNullValue errors
        assertThrows(C3rIllegalArgumentException.class, () ->
                minimalConfigBuilder(parquetPath.toString())
                        .csvInputNullValue("")
                        .build());

        // parquet file with csvOutputNullValue errors
        assertThrows(C3rIllegalArgumentException.class, () ->
                minimalConfigBuilder(parquetPath.toString())
                        .csvOutputNullValue("")
                        .build());
    }
}
