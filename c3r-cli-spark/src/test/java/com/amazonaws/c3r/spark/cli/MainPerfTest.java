// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.cli;

import com.amazonaws.c3r.cleanrooms.CleanRoomsDao;
import com.amazonaws.c3r.spark.cleanrooms.CleanRoomsDaoTestUtility;
import com.amazonaws.c3r.spark.io.CsvTestUtility;
import com.amazonaws.c3r.spark.utils.FileTestUtility;
import com.amazonaws.c3r.spark.utils.SparkSessionTestUtility;
import com.amazonaws.c3r.spark.utils.TableGeneratorTestUtility;
import com.amazonaws.c3r.spark.utils.TimingResultTestUtility;
import com.univocity.parsers.csv.CsvParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
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
        final String schemaPath = TableGeneratorTestUtility.generateSchema(columnCount, rowCount).toString();
        final Path dataPath = TableGeneratorTestUtility.generateCsv(entrySize, columnCount, rowCount);
        final long inputSizeBytes = dataPath.toFile().length();
        final Path marshalledPath = FileTestUtility.createTempDir();
        final Path unmarshalledPath = FileTestUtility.createTempDir();

        encArgs.setInput(dataPath.toString());
        encArgs.setSchema(schemaPath);
        encArgs.setOutput(marshalledPath.toString());

        final CleanRoomsDao cleanRoomsDao = CleanRoomsDaoTestUtility.generateMockDao();
        when(cleanRoomsDao.getCollaborationDataEncryptionMetadata(any())).thenReturn(encArgs.getClientSettings());
        long totalMarshalTimeSec = 0;
        for (int i = 0; i < repetitions; i++) {
            final long startTimeMs = System.currentTimeMillis();
            final int exitCode = EncryptMode.getApp(cleanRoomsDao, SparkSessionTestUtility.initSparkSession())
                    .execute(encArgs.toArrayWithoutMode());
            final long endTimeMs = System.currentTimeMillis();
            totalMarshalTimeSec = totalMarshalTimeSec + ((endTimeMs - startTimeMs) / 1000);
            assertEquals(0, exitCode);
        }

        final Path mergedMarshalledData = CsvTestUtility.mergeOutput(marshalledPath);

        final long marshalledSizeBytes = mergedMarshalledData.toFile().length();

        decArgs.setFailOnFingerprintColumns(false);
        decArgs.setInput(mergedMarshalledData.toString());
        decArgs.setOutput(unmarshalledPath.toString());

        // printCliArgs();
        long totalUnmarshalTimeSec = 0;
        for (int i = 0; i < repetitions; i++) {
            final long startTimeMs = System.currentTimeMillis();
            final int exitCode = DecryptMode.getApp(SparkSessionTestUtility.initSparkSession()).execute(decArgs.toArrayWithoutMode());
            final long endTimeMs = System.currentTimeMillis();
            totalUnmarshalTimeSec = totalUnmarshalTimeSec + ((endTimeMs - startTimeMs) / 1000);
            assertEquals(0, exitCode);
        }
        final Path mergedUnmarshalledData = CsvTestUtility.mergeOutput(unmarshalledPath);

        final long unmarshalledSizeBytes = mergedUnmarshalledData.toFile().length();

        final CsvParser parser = CsvTestUtility.getCsvParser(mergedUnmarshalledData.toString(), columnCount);
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
