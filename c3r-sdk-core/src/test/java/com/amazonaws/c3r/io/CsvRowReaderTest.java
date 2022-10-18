// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.Limits;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CsvRowReaderTest {

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
    // ColumnSchema Name -> ColumnSchema Value mappings used for convenient testing data
    // LinkedHashMap to ensure ordering of entries for tests that may care

    private final Map<String, String> exampleCsvEntries = new LinkedHashMap<>() {
        {
            put("foo", "foo");
            put("quoted-foo", "\"foo\"");
            put("blank", "");
            put("1space", " ");
            put("quoted-blank", "\"\"");
            put("quoted-1space", "\" \"");
            put("\\N", "\\N");
            put("quoted-\\N", "\"\\N\"");
            put("spaced-\\N", " \\N ");
            put("quoted-spaced-\\N", "\" \\N \"");
        }
    };

    private CsvRowReader cReader;

    private Path tempDir;

    private Path csvValuesPath;

    @BeforeEach
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("temp");
        tempDir.toFile().deleteOnExit();
        csvValuesPath = Files.createTempFile(tempDir, "csv-values", ".csv");
        csvValuesPath.toFile().deleteOnExit();
        writeTestData(csvValuesPath, exampleCsvEntries);
    }

    private void writeTestData(final Path path, final Map<String, String> data) throws IOException {
        final String headerRow = String.join(",", data.keySet());
        final String valueRow = String.join(",", data.values());
        Files.writeString(path,
                String.join("\n",
                        headerRow,
                        valueRow));
    }

    @AfterEach
    public void shutdown() {
        if (cReader != null) {
            cReader.close();
            cReader = null;
        }
    }

    @Test
    public void getHeadersTest() {
        cReader = CsvRowReader.builder().sourceName("../samples/csv/data_sample_without_quotes.csv").build();

        assertFalse(cReader.getHeaders().isEmpty());
        assertArrayEquals(new ColumnHeader[]{new ColumnHeader("firstname"), new ColumnHeader("lastname"), new ColumnHeader("address"),
                        new ColumnHeader("city"), new ColumnHeader("state"),
                        new ColumnHeader("phonenumber"), new ColumnHeader("title"), new ColumnHeader("level"),
                        new ColumnHeader("notes")},
                cReader.getHeaders().toArray());
    }

    @Test
    public void missingHeadersThrowsTest() throws IOException {
        final Path file = Files.createTempFile(tempDir, "data-without-headers", ".csv");
        file.toFile().deleteOnExit();
        assertThrowsExactly(
                C3rRuntimeException.class,
                () -> CsvRowReader.builder().sourceName(file.toFile().getAbsolutePath()).build());
    }

    @Test
    public void badCharEncodingTest() {
        assertThrowsExactly(C3rRuntimeException.class,
                () -> CsvRowReader.builder().sourceName("../samples/csv/nonUtf8Encoding.csv").build());
    }

    @Test
    public void rowsAreExpectedSizeTest() {
        cReader = CsvRowReader.builder().sourceName("../samples/csv/data_sample_without_quotes.csv").build();

        while (cReader.hasNext()) {
            final Row<CsvValue> row = cReader.next();
            assertEquals(row.size(), dataSampleHeaders.size());
        }
        assertNull(cReader.next());
    }

    @Test
    public void customNullValuesTest() {
        cReader = CsvRowReader.builder().sourceName("../samples/csv/data_sample_without_quotes.csv").inputNullValue("null").build();

        final ColumnHeader notesColumn = new ColumnHeader("Notes");
        boolean hasNull = false;
        while (cReader.hasNext()) {
            final Row<CsvValue> row = cReader.next();
            hasNull |= row.getValue(notesColumn).toString() == null;
        }
        assertTrue(hasNull);
    }

    private Row<CsvValue> readCsvValuesPathRow(final String inputNullValue) {
        cReader = CsvRowReader.builder().sourceName(csvValuesPath.toString()).inputNullValue(inputNullValue).build();
        final List<Row<CsvValue>> rows = new ArrayList<>();
        while (cReader.hasNext()) {
            rows.add(cReader.next());
        }
        // sanity check the file is still just one row...
        assertEquals(1, rows.size());
        return rows.get(0);
    }

    @Test
    public void omittedInputNullValueTest() {
        // When NULL is not specified, any blank (i.e., no non-whitespace characters) quoted or unquoted
        // is treated as NULL.
        final Row<CsvValue> actual = readCsvValuesPathRow(null);
        final var expected = GeneralTestUtility.csvRow(
                "foo", "foo",
                "quoted-foo", "foo",
                "blank", null,
                "1space", null,
                "quoted-blank", null,
                "quoted-1space", " ",
                "\\N", "\\N",
                "quoted-\\N", "\\N",
                "spaced-\\N", "\\N",
                "quoted-spaced-\\N", " \\N "
        );

        assertEquals(expected, actual);
    }

    @Test
    public void emptyStringInputNullValueTest() {
        // When NULL is specified as just space characters (e.g., `""`, `" "`, etc), any unquoted
        // blank entries will be considered NULL, but quoted values (`"\"...\""`) will not
        // regardless of content between the quotes.
        final Row<CsvValue> actual = readCsvValuesPathRow("");
        final var expected = GeneralTestUtility.csvRow(
                "foo", "foo",
                "quoted-foo", "foo",
                "blank", null,
                "1space", null,
                "quoted-blank", "",
                "quoted-1space", " ",
                "\\N", "\\N",
                "quoted-\\N", "\\N",
                "spaced-\\N", "\\N",
                "quoted-spaced-\\N", " \\N "
        );

        assertEquals(expected, actual);
    }

    @Test
    public void spaceStringInputNullValueTest() {
        // specifying `" "` is equivalent to specifying `""`: any unquoted blank string is NULL
        final Row<CsvValue> actual = readCsvValuesPathRow(" ");
        final var expected = GeneralTestUtility.csvRow(
                "foo", "foo",
                "quoted-foo", "foo",
                "blank", null,
                "1space", null,
                "quoted-blank", "",
                "quoted-1space", " ",
                "\\N", "\\N",
                "quoted-\\N", "\\N",
                "spaced-\\N", "\\N",
                "quoted-spaced-\\N", " \\N "
        );

        assertEquals(expected, actual);
    }

    @Test
    public void quotedEmptyStringInputNullValueTest() {
        // specifying `"\"\""` means only exactly that syntactic value is parsed as NULL
        final Row<CsvValue> actual = readCsvValuesPathRow("\"\"");
        final var expected = GeneralTestUtility.csvRow(
                "foo", "foo",
                "quoted-foo", "foo",
                "blank", "",
                "1space", "",
                "quoted-blank", null,
                "quoted-1space", " ",
                "\\N", "\\N",
                "quoted-\\N", "\\N",
                "spaced-\\N", "\\N",
                "quoted-spaced-\\N", " \\N "
        );

        assertEquals(expected, actual);
    }

    @Test
    public void customStringInputNullValue1Test() {
        final Row<CsvValue> actual = readCsvValuesPathRow("foo");
        final var expected = GeneralTestUtility.csvRow(
                "foo", null,
                "quoted-foo", null,
                "blank", "",
                "1space", "",
                "quoted-blank", "",
                "quoted-1space", " ",
                "\\N", "\\N",
                "quoted-\\N", "\\N",
                "spaced-\\N", "\\N",
                "quoted-spaced-\\N", " \\N "
        );

        assertEquals(expected, actual);
    }

    @Test
    public void customStringInputNullValue2Test() {
        final Row<CsvValue> actual = readCsvValuesPathRow("\\N");
        final var expected = GeneralTestUtility.csvRow(
                "foo", "foo",
                "quoted-foo", "foo",
                "blank", "",
                "1space", "",
                "quoted-blank", "",
                "quoted-1space", " ",
                "\\N", null,
                "quoted-\\N", null,
                "spaced-\\N", null,
                "quoted-spaced-\\N", " \\N "
        );

        assertEquals(expected, actual);
    }

    @Test
    public void maxColumnCountTest() throws IOException {
        final Map<String, String> columns = new HashMap<>();
        // The <= is to make sure there's 1 more than the allowed max columns
        for (int i = 0; i <= CsvRowReader.MAX_COLUMN_COUNT; i++) {
            columns.put("column" + i, "value" + i);
        }
        writeTestData(csvValuesPath, columns);
        // header row is enough to throw error on MAX_COLUMN_COUNT
        assertThrowsExactly(C3rRuntimeException.class, () -> CsvRowReader.builder().sourceName(csvValuesPath.toString()).build());
    }

    @Test
    public void maxVarcharByteCountHeaderTest() throws IOException {
        final Map<String, String> columns = new HashMap<>();
        final byte[] varcharBytes = new byte[Limits.GLUE_MAX_HEADER_UTF8_BYTE_LENGTH + 1];
        Arrays.fill(varcharBytes, (byte) 'a');
        final String oversizedVarchar = new String(varcharBytes, StandardCharsets.UTF_8);
        columns.put(oversizedVarchar, "value");

        writeTestData(csvValuesPath, columns);
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> CsvRowReader.builder().sourceName(csvValuesPath.toString()).build());
    }

    @Test
    public void getCsvColumnCountTest() {
        assertEquals(
                dataSampleHeaders.size(),
                CsvRowReader.getCsvColumnCount("../samples/csv/data_sample_without_quotes.csv", StandardCharsets.UTF_8));
    }

    @Test
    public void getCsvColumnCountInvalidHeadersInFirstRowTest() throws IOException {
        final List<String> fileContentWith6Columns = List.of(
                ",,,,,",
                "\"\",\"\",\"\",\"\",\"\",\"\"",
                "\",\",\",\",\",\",\",\",\",\",\",\"",
                ",,,,,\n",
                "a".repeat(Limits.GLUE_MAX_HEADER_UTF8_BYTE_LENGTH + 1) + ",,,,,"
        );
        for (var content : fileContentWith6Columns) {
            final Path nullsCsv = Files.createTempFile("nulls", ".csv");
            nullsCsv.toFile().deleteOnExit();
            Files.writeString(nullsCsv, content, StandardCharsets.UTF_8);
            // ensure even when the first row has invalid headers, this method works as expected (i.e., that we're not using
            // ColumnHeader on accident somewhere when we don't need to)
            assertEquals(
                    6,
                    CsvRowReader.getCsvColumnCount(nullsCsv.toString(), StandardCharsets.UTF_8));
            Files.delete(nullsCsv);
        }
    }

    @Test
    public void getCsvColumnCountEmptyFileTest() throws IOException {
        final Path emptyFile = Files.createTempFile("missing", ".csv");
        assertEquals(0, Files.size(emptyFile));
        assertThrows(C3rRuntimeException.class, () ->
                CsvRowReader.getCsvColumnCount(emptyFile.toString(), StandardCharsets.UTF_8));
    }

    @Test
    public void getCsvColumnCountMissingFileTest() throws IOException {
        final Path missingFile = Files.createTempFile(tempDir, "missing", ".csv");
        Files.deleteIfExists(missingFile);
        assertThrows(C3rRuntimeException.class, () ->
                CsvRowReader.getCsvColumnCount(missingFile.toString(), StandardCharsets.UTF_8));
    }

    @Test
    public void getCsvColumnCountNonCsvFileTest() {
        assertThrows(C3rRuntimeException.class, () ->
                CsvRowReader.getCsvColumnCount("../samples/parquet/data_sample.parquet", StandardCharsets.UTF_8));
    }
}
