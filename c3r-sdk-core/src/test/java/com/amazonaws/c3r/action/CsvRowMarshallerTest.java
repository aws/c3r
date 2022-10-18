// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.action;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.EncryptConfig;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.amazonaws.c3r.utils.GeneralTestUtility.TEST_CONFIG_DATA_SAMPLE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CsvRowMarshallerTest {
    private Path tempDir;

    @BeforeEach
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("temp");
        tempDir.toFile().deleteOnExit();
    }

    @Test
    public void validateRejectNonCsvFormatTest() {
        final File encOutput = tempDir.resolve("endToEndMarshalOut.unknown").toFile();
        encOutput.deleteOnExit();
        final var configBuilder = EncryptConfig.builder()
                .sourceFile(TEST_CONFIG_DATA_SAMPLE.getInput())
                .targetFile(encOutput.getAbsolutePath())
                .secretKey(TEST_CONFIG_DATA_SAMPLE.getKey())
                .salt(TEST_CONFIG_DATA_SAMPLE.getSalt())
                .tempDir(tempDir.toAbsolutePath().toString())
                .settings(ClientSettings.lowAssuranceMode())
                .tableSchema(GeneralTestUtility.CONFIG_SAMPLE)
                .overwrite(true);

        assertThrows(C3rIllegalArgumentException.class, () ->
                CsvRowMarshaller.newInstance(configBuilder.fileFormat(FileFormat.PARQUET).build()));
        assertDoesNotThrow(() ->
                CsvRowMarshaller.newInstance(configBuilder.fileFormat(FileFormat.CSV).build()));
    }
}
