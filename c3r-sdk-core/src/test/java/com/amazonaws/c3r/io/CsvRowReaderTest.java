// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.Limits;
import com.amazonaws.c3r.utils.FileTestUtility;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CsvRowReaderTest {

    private final List<String> dataSampleRawHeaderNames =
            List.of("FirstName",
                    "LastName",
                    "Address",
                    "City",
                    "State",
                    "PhoneNumber",
                    "Title",
                    "Level",
                    "Notes"
            );

    private final List<ColumnHeader> dataSampleHeaders =
            dataSampleRawHeaderNames.stream().map(ColumnHeader::new).collect(Collectors.toList());

    private final List<ColumnHeader> dataSampleHeadersNoNormalization =
            dataSampleRawHeaderNames.stream().map(ColumnHeader::ofRaw).collect(Collectors.toList());

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
            put("backslash-N", "\\N");
            put("quoted-backslash-N", "\"\\N\"");
            put("spaced-backslash-N", " \\N ");
            put("quoted-spaced-backslash-N", "\" \\N \"");
        }
    };

    private CsvRowReader cReader;

    private Path input;

    @BeforeEach
    public void setup() throws IOException {
        input = FileTestUtility.createTempFile("input", ".csv");
        writeTestData(input, exampleCsvEntries);
    }

    private void writeTestData(final Path path, final Map<String, String> data) throws IOException {
        final String headerRow = String.join(",", data.keySet());
        final String valueRow = String.join(",", data.values());
        Files.writeString(path,
                String.join("\n",
                        headerRow,
                        valueRow),
                StandardCharsets.UTF_8);
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
        assertEquals(dataSampleHeaders, cReader.getHeaders());
    }

    @Test
    public void getHeadersNormalizationTest() {
        // explicitly normalize the headers and make sure they match the expected values
        cReader = CsvRowReader.builder().sourceName("../samples/csv/data_sample_without_quotes.csv").skipHeaderNormalization(false).build();
        assertEquals(dataSampleHeaders, cReader.getHeaders());
    }

    @Test
    public void getHeadersNoNormalizationTest() {
        cReader = CsvRowReader.builder().sourceName("../samples/csv/data_sample_without_quotes.csv").skipHeaderNormalization(true).build();
        assertEquals(dataSampleHeadersNoNormalization, cReader.getHeaders());
    }

    @Test
    public void missingHeadersThrowsTest() throws IOException {
        final String input = FileTestUtility.createTempFile("headerless", ".csv").toString();
        assertThrowsExactly(
                C3rRuntimeException.class,
                () -> CsvRowReader.builder().sourceName(input).build());
    }

    @Test
    public void badCharEncodingTest() {
        assertThrowsExactly(C3rRuntimeException.class,
                () -> CsvRowReader.builder().sourceName("../samples/csv/nonUtf8Encoding.csv").build());
    }

    @Test
    public void tooManyColumnsTest() {
        assertThrowsExactly(C3rRuntimeException.class,
                () -> CsvRowReader.builder().sourceName("../samples/csv/one_row_too_many_columns.csv").build());
    }

    @Test
    public void tooFewColumnsTest() {
        assertThrowsExactly(C3rRuntimeException.class,
                () -> CsvRowReader.builder().sourceName("../samples/csv/one_row_too_few_columns.csv").build());
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
        cReader = CsvRowReader.builder().sourceName(input.toString()).inputNullValue(inputNullValue).build();
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
                "backslash-N", "\\N",
                "quoted-backslash-N", "\\N",
                "spaced-backslash-N", "\\N",
                "quoted-spaced-backslash-N", " \\N "
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
                "backslash-N", "\\N",
                "quoted-backslash-N", "\\N",
                "spaced-backslash-N", "\\N",
                "quoted-spaced-backslash-N", " \\N "
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
                "backslash-N", "\\N",
                "quoted-backslash-N", "\\N",
                "spaced-backslash-N", "\\N",
                "quoted-spaced-backslash-N", " \\N "
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
                "backslash-N", "\\N",
                "quoted-backslash-N", "\\N",
                "spaced-backslash-N", "\\N",
                "quoted-spaced-backslash-N", " \\N "
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
                "backslash-N", "\\N",
                "quoted-backslash-N", "\\N",
                "spaced-backslash-N", "\\N",
                "quoted-spaced-backslash-N", " \\N "
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
                "backslash-N", null,
                "quoted-backslash-N", null,
                "spaced-backslash-N", null,
                "quoted-spaced-backslash-N", " \\N "
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
        writeTestData(input, columns);
        // header row is enough to throw error on MAX_COLUMN_COUNT
        assertThrowsExactly(C3rRuntimeException.class, () -> CsvRowReader.builder().sourceName(input.toString()).build());
    }

    @Test
    public void maxVarcharByteCountHeaderTest() throws IOException {
        final Map<String, String> columns = new HashMap<>();
        final byte[] varcharBytes = new byte[Limits.AWS_CLEAN_ROOMS_HEADER_MAX_LENGTH + 1];
        Arrays.fill(varcharBytes, (byte) 'a');
        final String oversizedVarchar = new String(varcharBytes, StandardCharsets.UTF_8);
        columns.put(oversizedVarchar, "value");

        writeTestData(input, columns);
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> CsvRowReader.builder().sourceName(input.toString()).build());
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
                "a".repeat(Limits.AWS_CLEAN_ROOMS_HEADER_MAX_LENGTH + 1) + ",,,,,"
        );
        for (var content : fileContentWith6Columns) {
            final Path nullsCsv = FileTestUtility.createTempFile();
            Files.writeString(nullsCsv, content, StandardCharsets.UTF_8);
            // ensure even when the first row has invalid headers, this method works as expected (i.e., that we're not using
            // ColumnHeader on accident somewhere when we don't need to)
            assertEquals(
                    6,
                    CsvRowReader.getCsvColumnCount(nullsCsv.toString(), StandardCharsets.UTF_8));
        }
    }

    @Test
    public void getCsvColumnCountEmptyFileTest() throws IOException {
        final Path emptyFile = FileTestUtility.createTempFile("missing", ".csv");
        assertEquals(0, Files.size(emptyFile));
        assertThrows(C3rRuntimeException.class, () ->
                CsvRowReader.getCsvColumnCount(emptyFile.toString(), StandardCharsets.UTF_8));
    }

    @Test
    public void getCsvColumnCountMissingFileTest() throws IOException {
        final String missingFile = FileTestUtility.resolve("missing.csv").toString();
        assertThrows(C3rRuntimeException.class, () ->
                CsvRowReader.getCsvColumnCount(missingFile, StandardCharsets.UTF_8));
    }

    @Test
    public void getCsvColumnCountNonCsvFileTest() {
        assertThrows(C3rRuntimeException.class, () ->
                CsvRowReader.getCsvColumnCount("../samples/parquet/data_sample.parquet", StandardCharsets.UTF_8));
    }
}
