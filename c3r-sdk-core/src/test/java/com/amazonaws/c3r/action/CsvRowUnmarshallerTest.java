// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.action;

import com.amazonaws.c3r.config.DecryptConfig;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.utils.FileTestUtility;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.amazonaws.c3r.utils.GeneralTestUtility.TEST_CONFIG_DATA_SAMPLE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CsvRowUnmarshallerTest {

    @Test
    public void validateRejectNonCsvFormatTest() throws IOException {
        final String output = FileTestUtility.resolve("endToEndMarshalOut.unknown").toString();
        final var configBuilder = DecryptConfig.builder()
                .sourceFile(TEST_CONFIG_DATA_SAMPLE.getInput())
                .targetFile(output)
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
