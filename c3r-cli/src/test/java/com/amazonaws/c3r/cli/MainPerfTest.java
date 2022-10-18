// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cli;

import com.amazonaws.c3r.cleanrooms.CleanRoomsDao;
import com.amazonaws.c3r.io.CsvTestUtility;
import com.amazonaws.c3r.utils.TableGeneratorTestUtility;
import com.amazonaws.c3r.utils.TimingResultTestUtility;
import com.univocity.parsers.csv.CsvParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MainPerfTest {
    private EncryptCliConfigTestUtility encArgs;

    private DecryptCliConfigTestUtility decArgs;

    @BeforeEach
    public void setup() {
        encArgs = EncryptCliConfigTestUtility.defaultTestArgs();
        encArgs.setAllowDuplicates(true);

        decArgs = DecryptCliConfigTestUtility.defaultTestArgs();
        decArgs.setFailOnFingerprintColumns(false);
    }

    public TimingResultTestUtility timeCsvRoundTrips(final int repetitions, final int entrySize, final int columnCount, final long rowCount)
            throws IOException {
        final var testDir = Files.createTempDirectory("temp");
        testDir.toFile().deleteOnExit();
        final var paths = TableGeneratorTestUtility.generateTestData(entrySize, columnCount, rowCount, testDir);
        final long inputSizeBytes = paths.data.toFile().length();
        final Path marshalledPath = java.nio.file.Files.createTempFile(
                testDir,
                TableGeneratorTestUtility.filePrefix(columnCount, rowCount),
                ".marshalled.csv");
        final Path unmarshalledPath = java.nio.file.Files.createTempFile(
                testDir,
                TableGeneratorTestUtility.filePrefix(columnCount, rowCount),
                ".unmarshalled.csv");

        paths.data.toFile().deleteOnExit();
        paths.schema.toFile().deleteOnExit();
        marshalledPath.toFile().deleteOnExit();
        unmarshalledPath.toFile().deleteOnExit();

        encArgs.setInput(paths.data.toString());
        encArgs.setSchema(paths.schema.toString());
        encArgs.setOutput(marshalledPath.toString());

        final CleanRoomsDao cleanRoomsDao = mock(CleanRoomsDao.class);
        when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(encArgs.getClientSettings());
        long totalMarshalTimeSec = 0;
        for (int i = 0; i < repetitions; i++) {
            final long startTimeMs = System.currentTimeMillis();
            final int exitCode = EncryptMode.getApp(cleanRoomsDao).execute(encArgs.toArrayWithoutMode());
            final long endTimeMs = System.currentTimeMillis();
            totalMarshalTimeSec = totalMarshalTimeSec + ((endTimeMs - startTimeMs) / 1000);
            assertEquals(0, exitCode);
        }
        final long marshalledSizeBytes = marshalledPath.toFile().length();

        decArgs.setFailOnFingerprintColumns(false);
        decArgs.setInput(marshalledPath.toString());
        decArgs.setOutput(unmarshalledPath.toString());

        // printCliArgs();
        long totalUnmarshalTimeSec = 0;
        for (int i = 0; i < repetitions; i++) {
            final long startTimeMs = System.currentTimeMillis();
            final int exitCode = Main.getApp().execute(decArgs.toArray());
            final long endTimeMs = System.currentTimeMillis();
            totalUnmarshalTimeSec = totalUnmarshalTimeSec + ((endTimeMs - startTimeMs) / 1000);
            assertEquals(0, exitCode);
        }
        final long unmarshalledSizeBytes = unmarshalledPath.toFile().length();

        final CsvParser parser = CsvTestUtility.getCsvParser(unmarshalledPath.toString(), columnCount);
        parser.parseNext(); // skip the header
        long readRows = 0;
        String[] row = parser.parseNext();
        while (row != null) {
            assertEquals(columnCount, row.length);
            readRows++;
            row = parser.parseNext();
        }
        assertEquals(rowCount, readRows);

        return TimingResultTestUtility.builder()
                .charsPerEntry(entrySize)
                .columnCount(columnCount)
                .rowCount(rowCount)
                .inputSizeBytes(inputSizeBytes)
                .marshalTimeSec(totalMarshalTimeSec / repetitions)
                .marshalledSizeBytes(marshalledSizeBytes)
                .unmarshalTimeSec(totalUnmarshalTimeSec / repetitions)
                .unmarshalledSizeBytes(unmarshalledSizeBytes)
                .build();
    }

    @Test
    public void timeVariousColRowSizes() throws IOException {
        final int[] columnCounts = {3, 6};
        final long[] rowCounts = {100, 1000};
        final int repetitions = 1;
        final int entrySize = 20;
        for (var nCols : columnCounts) {
            for (var nRows : rowCounts) {
                timeCsvRoundTrips(repetitions, entrySize, nCols, nRows);
            }
        }
    }

}
