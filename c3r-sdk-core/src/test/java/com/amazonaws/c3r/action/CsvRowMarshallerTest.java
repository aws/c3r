// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.action;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.EncryptConfig;
import com.amazonaws.c3r.config.MappedTableSchema;
import com.amazonaws.c3r.config.Pad;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.io.FileFormat;
import com.amazonaws.c3r.utils.FileTestUtility;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import nl.altindag.log.LogCaptor;
import nl.altindag.log.model.LogEvent;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.amazonaws.c3r.utils.GeneralTestUtility.TEST_CONFIG_DATA_SAMPLE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CsvRowMarshallerTest {

    @Test
    public void validateRejectNonCsvFormatTest() throws IOException {
        final String tempDir = FileTestUtility.createTempDir().toString();
        final String output = FileTestUtility.resolve("endToEndMarshalOut.unknown").toString();
        final var configBuilder = EncryptConfig.builder()
                .sourceFile(TEST_CONFIG_DATA_SAMPLE.getInput())
                .targetFile(output)
                .secretKey(TEST_CONFIG_DATA_SAMPLE.getKey())
                .salt(TEST_CONFIG_DATA_SAMPLE.getSalt())
                .tempDir(tempDir)
                .settings(ClientSettings.lowAssuranceMode())
                .tableSchema(GeneralTestUtility.CONFIG_SAMPLE)
                .overwrite(true);

        assertThrows(C3rIllegalArgumentException.class, () ->
                CsvRowMarshaller.newInstance(configBuilder.fileFormat(FileFormat.PARQUET).build()));
        assertDoesNotThrow(() ->
                CsvRowMarshaller.newInstance(configBuilder.fileFormat(FileFormat.CSV).build()));
    }

    @Test
    public void validateWarnCustomNullsWithNoTargetCleartext() {
        final List<LogEvent> logEvents;
        final List<ColumnSchema> columnSchemas = new ArrayList<>();
        columnSchemas.add(ColumnSchema.builder()
                .sourceHeader(new ColumnHeader("source"))
                .targetHeader(new ColumnHeader("target"))
                .type(ColumnType.SEALED)
                .pad(Pad.DEFAULT)
                .build());
        final TableSchema schema = new MappedTableSchema(columnSchemas);
        try (LogCaptor logCaptor = LogCaptor.forName("ROOT")) {
            CsvRowMarshaller.validate("custom null value", schema);
            logEvents = logCaptor.getLogEvents();
        }
        final LogEvent warningEvent = logEvents.get(logEvents.size() - 1); // The last message is the warning
        final String outputMessage = warningEvent.getFormattedMessage();
        assertTrue(outputMessage.contains("Received a custom output null value `custom null value`, but no cleartext columns were found. " +
                "It will be ignored."));
    }
}
