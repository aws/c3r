// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.csv;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.io.CsvRowWriter;
import com.amazonaws.c3r.spark.utils.FileTestUtility;
import com.amazonaws.c3r.spark.utils.SparkSessionTestUtility;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkCsvWriterTest {
    private final List<ColumnHeader> dataSampleHeaders =
            Stream.of("FirstName",
                            "LastName",
                            "Address",
                            "City",
                            "State",
                            "PhoneNumber",
                            "Title",
                            "Level",
                            "Notes"
                    )
                    .map(ColumnHeader::new)
                    .collect(Collectors.toList());

    private final SparkSession session = SparkSessionTestUtility.initSparkSession();

    private Path tempInputFile;

    private Path tempOutputDir;

    @BeforeEach
    public void setup() throws IOException {
        tempInputFile = FileTestUtility.createTempFile("temp", ".csv");
        tempOutputDir = FileTestUtility.createTempDir();
    }

    @Test
    public void initWriterHeadersTest() {
        final Map<String, String> properties = new HashMap<>();
        final String headers = dataSampleHeaders.stream().map(ColumnHeader::toString).collect(Collectors.joining(","));
        properties.put("headers", headers);
        properties.put("path", tempOutputDir.toString());
        final CsvRowWriter writer = SparkCsvWriter.initWriter(0, properties);
        assertEquals(dataSampleHeaders.size(), writer.getHeaders().size());
        assertTrue(dataSampleHeaders.containsAll(writer.getHeaders()));
    }

    @Test
    public void initWriterNoPathTest() {
        final Map<String, String> properties = new HashMap<>();
        final String headers = dataSampleHeaders.stream().map(ColumnHeader::toString).collect(Collectors.joining(","));
        properties.put("headers", headers);
        assertThrows(C3rRuntimeException.class, () -> SparkCsvWriter.initWriter(0, properties));
    }

    @Test
    public void initWriterTargetTest() {
        final Map<String, String> properties = new HashMap<>();
        final String headers = dataSampleHeaders.stream().map(ColumnHeader::toString).collect(Collectors.joining(","));
        properties.put("headers", headers);
        properties.put("path", tempOutputDir.toString());
        properties.put("sessionUuid", UUID.randomUUID().toString());
        final CsvRowWriter writer = SparkCsvWriter.initWriter(1, properties);
        String target = writer.getTargetName();
        target = target.substring(tempOutputDir.toString().length() + 1); // Remove dir path
        final String[] split = target.split("-");
        assertEquals(7, split.length); // UUID hyphens plus the initial.
        assertEquals("part", split[0]);
        assertEquals("00001", split[1]);
    }

    @Test
    public void quotedSpaceTest() throws IOException {
        final String singleRowQuotedSpace = "column\n\" \"";
        Files.writeString(tempInputFile, singleRowQuotedSpace);
        final Dataset<Row> originalDataset = SparkCsvReader.readInput(session,
                tempInputFile.toString(),
                null,
                null);
        SparkCsvWriter.writeOutput(originalDataset, tempOutputDir.toString(), null);
        final Dataset<Row> writtenDataset = SparkCsvReader.readInput(session,
                tempOutputDir.toString(),
                null,
                null);
        final List<Row> originalData = originalDataset.collectAsList();
        final List<Row> writtenData = writtenDataset.collectAsList();

        // ensure data read in doesn't change when written out
        assertEquals(originalData.get(0).getString(0), writtenData.get(0).get(0));
        assertEquals(" ", writtenData.get(0).get(0));
    }

    @Test
    public void unquotedBlankTest() throws IOException {
        final String singleRowQuotedSpace = "column, column2\n ,";
        Files.writeString(tempInputFile, singleRowQuotedSpace);
        final Dataset<Row> originalDataset = SparkCsvReader.readInput(session,
                tempInputFile.toString(),
                null,
                null);
        SparkCsvWriter.writeOutput(originalDataset, tempOutputDir.toString(), null);
        final Dataset<Row> writtenDataset = SparkCsvReader.readInput(session,
                tempOutputDir.toString(),
                null,
                null);
        final List<Row> originalData = originalDataset.collectAsList();
        final List<Row> writtenData = writtenDataset.collectAsList();

        // ensure data read in doesn't change when written out
        assertNull(originalData.get(0).get(0));
        assertNull(originalData.get(0).get(1));
        assertNull(writtenData.get(0).get(0));
        assertNull(writtenData.get(0).get(1));
    }

    @Test
    public void customNullTest() throws IOException {
        final String singleRowQuotedSpace = "column, column2\ncolumn,";
        Files.writeString(tempInputFile, singleRowQuotedSpace);
        final Dataset<Row> originalDataset = SparkCsvReader.readInput(session,
                tempInputFile.toString(),
                "column",
                null);
        SparkCsvWriter.writeOutput(originalDataset, tempOutputDir.toString(), "column");
        final Dataset<Row> writtenDataset = SparkCsvReader.readInput(session,
                tempOutputDir.toString(),
                null,
                null);
        final List<Row> originalData = originalDataset.collectAsList();
        final List<Row> writtenData = writtenDataset.collectAsList();

        // ensure a column with a header that equals the custom null value is not dropped.
        assertEquals("column", originalDataset.columns()[0]);
        assertEquals("column", writtenDataset.columns()[0]);

        // ensure custom null respected
        assertNull(originalData.get(0).get(0));
        assertEquals("column", writtenData.get(0).get(0));
    }
}
