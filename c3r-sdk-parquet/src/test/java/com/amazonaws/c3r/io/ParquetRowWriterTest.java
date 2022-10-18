// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.utils.ParquetTestUtility;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;

import static com.amazonaws.c3r.io.ParquetRowReaderTest.validateRowsGetValueContent;
import static com.amazonaws.c3r.io.ParquetRowReaderTest.validateRowsGetValueNullContent;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ParquetRowWriterTest {

    @Test
    public void getTargetNameTest() throws IOException {
        final var inReader = new ParquetRowReader(ParquetTestUtility.PARQUET_1_ROW_PRIM_DATA_PATH);
        final var outPath = Files.createTempFile("1row_prim_data", ".parquet");
        Files.deleteIfExists(outPath);

        final ParquetRowWriter writer =
                ParquetRowWriter.builder().targetName(outPath.toString()).parquetSchema(inReader.getParquetSchema()).build();
        assertEquals(outPath.toString(), writer.getTargetName());
    }

    @Test
    public void getHeadersTest() throws IOException {
        final var inReader = new ParquetRowReader(ParquetTestUtility.PARQUET_1_ROW_PRIM_DATA_PATH);
        final var outPath = Files.createTempFile("1row_prim_data", ".parquet");
        Files.deleteIfExists(outPath);

        final ParquetRowWriter writer =
                ParquetRowWriter.builder().targetName(outPath.toString()).parquetSchema(inReader.getParquetSchema()).build();
        assertEquals(ParquetTestUtility.PARQUET_TEST_DATA_HEADERS, writer.getHeaders());
    }

    private void roundTripAssertEquals(final String inPath, final int rowCount, final boolean nonNullEntries)
            throws IOException {
        final var inReader = new ParquetRowReader(inPath);
        final var inRows = ParquetTestUtility.readAllRows(inReader);
        assertEquals(rowCount, inRows.size());
        if (nonNullEntries) {
            validateRowsGetValueContent(inRows);
        } else {
            validateRowsGetValueNullContent(inRows);
        }

        final var outPath = Files.createTempFile(rowCount + "rows_prim_data", ".parquet");
        outPath.toFile().deleteOnExit();
        Files.deleteIfExists(outPath);

        final ParquetRowWriter writer =
                ParquetRowWriter.builder().targetName(outPath.toString()).parquetSchema(inReader.getParquetSchema()).build();

        for (var row : inRows) {
            writer.writeRow(row);
        }

        writer.close();
        writer.flush();

        final var outReader = new ParquetRowReader(outPath.toString());
        final var outRows = ParquetTestUtility.readAllRows(outReader);

        assertEquals(rowCount, outRows.size());
        for (int i = 0; i < rowCount; i++) {
            assertEquals(inRows.get(i), outRows.get(i), "row " + i);
        }

        outReader.close();
        inReader.close();
        Files.deleteIfExists(outPath);
    }

    @Test
    public void roundTrip1RowTest() throws IOException {
        roundTripAssertEquals(ParquetTestUtility.PARQUET_1_ROW_PRIM_DATA_PATH, 1, true);
    }

    @Test
    public void roundTrip100Rows1GroupTest() throws IOException {
        roundTripAssertEquals(ParquetTestUtility.PARQUET_100_ROWS_PRIM_DATA_PATH, 100, true);
    }

    @Test
    public void roundTrip100Rows10GroupsTest() throws IOException {
        roundTripAssertEquals(ParquetTestUtility.PARQUET_100_ROWS_10_GROUPS_PRIM_DATA_PATH, 100, true);
    }

    @Test
    public void roundTrip1NullRowTest() throws IOException {
        roundTripAssertEquals(ParquetTestUtility.PARQUET_NULL_1_ROW_PRIM_DATA_PATH, 1, false);
    }

    @Test
    public void roundTrip100NullRows1GroupTest() throws IOException {
        roundTripAssertEquals(ParquetTestUtility.PARQUET_NULL_100_ROWS_PRIM_DATA_PATH, 100, false);
    }
}
