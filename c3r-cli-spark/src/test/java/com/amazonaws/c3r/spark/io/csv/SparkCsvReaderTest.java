// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.csv;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.io.CsvRowReader;
import com.amazonaws.c3r.spark.config.SparkConfig;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazonaws.c3r.spark.utils.GeneralTestUtility.DATA_SAMPLE_HEADERS;
import static com.amazonaws.c3r.spark.utils.GeneralTestUtility.DATA_SAMPLE_HEADERS_NO_NORMALIZATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkCsvReaderTest {

    private final SparkSession session = SparkSessionTestUtility.initSparkSession();

    private Path tempFile;

    @BeforeEach
    public void setup() throws IOException {
        tempFile = FileTestUtility.createTempFile();
    }

    @Test
    public void initReaderHeadersTest() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("path", "../samples/csv/data_sample_with_quotes.csv");
        final CsvRowReader reader = SparkCsvReader.initReader(properties);
        assertEquals(
                DATA_SAMPLE_HEADERS.stream().map(ColumnHeader::toString).sorted().collect(Collectors.toList()),
                reader.getHeaders().stream().map(ColumnHeader::toString).sorted().collect(Collectors.toList()));
    }

    @Test
    public void initReaderHeadersNoNormalizationTest() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("path", "../samples/csv/data_sample_with_quotes.csv");
        properties.put(SparkConfig.PROPERTY_KEY_SKIP_HEADER_NORMALIZATION, "true");
        final CsvRowReader reader = SparkCsvReader.initReader(properties);
        assertEquals(
                DATA_SAMPLE_HEADERS_NO_NORMALIZATION.stream().map(ColumnHeader::toString).sorted().collect(Collectors.toList()),
                reader.getHeaders().stream().map(ColumnHeader::toString).sorted().collect(Collectors.toList()));
    }

    @Test
    public void initReaderNoHeadersTest() {
        final List<ColumnHeader> customDataSampleHeaders =
                Stream.of("FirstNameCustom",
                                "LastNameCustom",
                                "AddressCustom",
                                "CityCustom",
                                "StateCustom",
                                "PhoneNumberCustom",
                                "TitleCustom",
                                "LevelCustom",
                                "NotesCustom"
                        )
                        .map(ColumnHeader::new)
                        .collect(Collectors.toList());
        final Map<String, String> properties = new HashMap<>();
        properties.put("path", "../samples/csv/data_sample_no_headers.csv");
        final String customHeader = customDataSampleHeaders.stream().map(ColumnHeader::toString).collect(Collectors.joining(","));
        properties.put("headers", customHeader);
        final CsvRowReader reader = SparkCsvReader.initReader(properties);
        assertEquals(customDataSampleHeaders.size(), reader.getHeaders().size());
        assertTrue(customDataSampleHeaders.containsAll(reader.getHeaders()));
    }

    @Test
    public void initReaderNoPathTest() {
        final Map<String, String> properties = new HashMap<>();
        assertThrows(C3rRuntimeException.class, () -> SparkCsvReader.initReader(properties));
    }

    @Test
    public void inputDirectoryTest() throws IOException {
        final Path tempDir = FileTestUtility.createTempDir();
        final Path file1 = tempDir.resolve("file1.csv");
        Files.writeString(file1, "column,column2\nfoo,bar");
        final Path file2 = tempDir.resolve("file2.csv");
        Files.writeString(file2, "column,column2\nbaz,buzz");
        final List<Row> fullDataset = SparkCsvReader.readInput(session,
                        tempDir.toString(),
                        null,
                        null)
                .collectAsList();
        final List<Row> dataset1 = SparkCsvReader.readInput(session,
                        file1.toString(),
                        null,
                        null)
                .collectAsList();
        final List<Row> dataset2 = SparkCsvReader.readInput(session,
                        file2.toString(),
                        null,
                        null)
                .collectAsList();
        assertTrue(fullDataset.containsAll(dataset1));
        assertTrue(fullDataset.containsAll(dataset2));
    }

    @Test
    public void inputNestedDirectoryTest() throws IOException {
        final Path tempDir = FileTestUtility.createTempDir();
        final Path file1 = tempDir.resolve("file1.csv");
        Files.writeString(file1, "column,column2\nfoo,bar");
        final Path nestedTempDir = tempDir.resolve("nested");
        Files.createDirectory(nestedTempDir);
        final Path file2 = nestedTempDir.resolve("file2.csv");
        Files.writeString(file2, "column,column2\nbaz,buzz");
        final List<Row> fullDataset = SparkCsvReader.readInput(session,
                        tempDir.toString(),
                        null,
                        null)
                .collectAsList();
        final List<Row> dataset1 = SparkCsvReader.readInput(session,
                        file1.toString(),
                        null,
                        null)
                .collectAsList();
        final List<Row> dataset2 = SparkCsvReader.readInput(session,
                        file2.toString(),
                        null,
                        null)
                .collectAsList();
        assertTrue(fullDataset.containsAll(dataset1));
        // recursion currently not supported
        assertFalse(fullDataset.containsAll(dataset2));
    }

    @Test
    public void inputDirectoryDuplicatesTest() throws IOException {
        final Path tempDir = FileTestUtility.createTempDir();
        final Path file1 = tempDir.resolve("file1.csv");
        final String duplicateFileContents = "column,column2\nfoo,bar";
        Files.writeString(file1, duplicateFileContents);
        final Path file2 = tempDir.resolve("file2.csv");
        Files.writeString(file2, duplicateFileContents);
        final List<Row> fullDataset = SparkCsvReader.readInput(session,
                        tempDir.toString(),
                        null,
                        null)
                .collectAsList();
        final List<Row> dataset1 = SparkCsvReader.readInput(session,
                        file1.toString(),
                        null,
                        null)
                .collectAsList();
        final List<Row> dataset2 = SparkCsvReader.readInput(session,
                        file2.toString(),
                        null,
                        null)
                .collectAsList();
        assertTrue(fullDataset.containsAll(dataset1));
        assertTrue(fullDataset.containsAll(dataset2));
        assertEquals(2, fullDataset.size());
    }

    @Test
    public void inputDirectoryUnrelatedDatasetsTest() throws IOException {
        final Path tempDir = FileTestUtility.createTempDir();
        final Path file1 = tempDir.resolve("file1.csv");
        Files.writeString(file1, "columnFoo,columnBar\nfoo,bar");
        final Path file2 = tempDir.resolve("file2.csv");
        Files.writeString(file2, "columnBaz,columnBuzz\nbaz,buzz");
        assertThrows(C3rRuntimeException.class, () -> SparkCsvReader.readInput(session,
                        tempDir.toString(),
                        null,
                        null)
                .collectAsList());
    }

    @Test
    public void quotedSpaceTest() throws IOException {
        final String singleRowQuotedSpace = "column\n\" \"";
        Files.writeString(tempFile, singleRowQuotedSpace);
        final List<Row> dataset = SparkCsvReader.readInput(session,
                        tempFile.toString(),
                        null,
                        null)
                .collectAsList();
        assertEquals(" ", dataset.get(0).getString(0));
    }

    @Test
    public void unquotedBlankTest() throws IOException {
        final String singleRowQuotedSpace = "column, column2\n ,";
        Files.writeString(tempFile, singleRowQuotedSpace);
        final List<Row> dataset = SparkCsvReader.readInput(session,
                        tempFile.toString(),
                        null,
                        null)
                .collectAsList();
        assertNull(dataset.get(0).get(0));
        assertNull(dataset.get(0).get(1));
    }

    @Test
    public void customNullTest() throws IOException {
        final String singleRowQuotedSpace = "column, column2\ncolumn,";
        Files.writeString(tempFile, singleRowQuotedSpace);
        final Dataset<Row> dataset = SparkCsvReader.readInput(session,
                tempFile.toString(),
                "column",
                null);
        // ensure a column with a header that equals the custom null value is not dropped.
        assertEquals("column", dataset.columns()[0]);

        final List<Row> data = dataset.collectAsList();
        // ensure custom null respected
        assertNull(data.get(0).get(0));

        // ensure empty value respected
        assertNotNull(data.get(0).get(1));
        assertEquals("", data.get(0).getString(1));
    }

    @Test
    public void maliciousColumnHeaderTest() throws IOException {
        final String singleRowQuotedSpace = "'; DROP ALL TABLES;";
        Files.writeString(tempFile, singleRowQuotedSpace);

        // Assert a malicious column header can't be read
        assertThrows(C3rIllegalArgumentException.class, () -> SparkCsvReader.readInput(session,
                tempFile.toString(),
                null,
                null));
    }
}
