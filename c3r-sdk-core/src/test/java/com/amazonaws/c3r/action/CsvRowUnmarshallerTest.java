// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.action;

import com.amazonaws.c3r.config.DecryptConfig;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.amazonaws.c3r.utils.GeneralTestUtility.TEST_CONFIG_DATA_SAMPLE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CsvRowUnmarshallerTest {

    private Path tempDir;

    @BeforeEach
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("temp");
        tempDir.toFile().deleteOnExit();
    }

    @Test
    public void validateRejectNonCsvFormatTest() {
        final File decOutput = tempDir.resolve("endToEndMarshalOut.unknown").toFile();
        decOutput.deleteOnExit();
        final var configBuilder = DecryptConfig.builder()
                .sourceFile(TEST_CONFIG_DATA_SAMPLE.getInput())
                .targetFile(decOutput.getAbsolutePath())
                .fileFormat(FileFormat.CSV)
                .secretKey(TEST_CONFIG_DATA_SAMPLE.getKey())
                .salt(TEST_CONFIG_DATA_SAMPLE.getSalt())
                .overwrite(true);

        assertThrows(C3rIllegalArgumentException.class, () ->
                CsvRowUnmarshaller.newInstance(configBuilder.fileFormat(FileFormat.PARQUET).build()));
        assertDoesNotThrow(() ->
                CsvRowUnmarshaller.newInstance(configBuilder.fileFormat(FileFormat.CSV).build()));
    }
}
