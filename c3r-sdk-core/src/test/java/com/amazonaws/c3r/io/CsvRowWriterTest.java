// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.CsvRow;
import com.amazonaws.c3r.utils.FileTestUtility;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CsvRowWriterTest {

    // ColumnSchema Name -> ColumnSchema Value mappings used for convenient testing data
    private static final CsvRow EXAMPLE_CSV_ROW = GeneralTestUtility.csvRow(
            "NULL", null,
            "foo", "foo",
            "foo-space-bar", "foo bar",
            "foo-newline-bar", "foo\nbar",
            "blank", "",
            "1space", " ",
            "quoted-blank", "\"\"",
            "quoted-1space", "\" \""
    );

    private static final List<ColumnHeader> HEADERS = Stream.of(
                    "NULL",
                    "foo",
                    "foo-space-bar",
                    "foo-newline-bar",
                    "blank",
                    "1space",
                    "quoted-blank",
                    "quoted-1space"
            ).map(ColumnHeader::new)
            .collect(Collectors.toList());

    private CsvRowWriter cWriter;

    private Path output;

    @BeforeEach
    public void setup() throws IOException {
        output = FileTestUtility.resolve("csv-values.csv");
        output.toFile().deleteOnExit();
    }

    @AfterEach
    public void shutdown() {
        if (cWriter != null) {
            cWriter.close();
            cWriter = null;
        }
    }

    private Map<String, String> readSingleCsvRow(final Path path) {
        final var rows = CsvTestUtility.readRows(path.toString());
        assertEquals(1, rows.size());
        return rows.get(0);
    }

    @Test
    public void defaultOutputNull_WriteRowTest() {
        cWriter = CsvRowWriter.builder()
                .headers(HEADERS)
                .targetName(output.toString())
                .fileCharset(StandardCharsets.UTF_8)
                .build();
        cWriter.writeRow(EXAMPLE_CSV_ROW);
        cWriter.close();

        final var actualRow = readSingleCsvRow(output);
        // assertRowEntryPredicates
        final var expectedRow = GeneralTestUtility.row(
                "null", "",
                "foo", "foo",
                "foo-space-bar", "\"foo bar\"",
                "foo-newline-bar", "\"foo\nbar\"",
                // Non-NULL blank gets written as `,"",` by default, so it can be distinguished from NULL
                "blank", "\"\"",
                // Writing out a space has to use quotes to preserve the space
                "1space", "\" \"",
                // Writing out quotes requires double quotes
                "quoted-blank", "\"\"\"\"",
                // Writing out quotes requires double quotes
                "quoted-1space", "\"\" \"\""
        );

        assertEquals(expectedRow, actualRow);
    }

    @Test
    public void customOutputNull_WriteRowTest() {
        cWriter = CsvRowWriter.builder()
                .headers(HEADERS)
                .outputNullValue("baz")
                .targetName(output.toString())
                .fileCharset(StandardCharsets.UTF_8)
                .build();
        cWriter.writeRow(EXAMPLE_CSV_ROW);
        cWriter.close();

        final var actualRow = readSingleCsvRow(output);
        // assertRowEntryPredicates
        final var expectedRow = GeneralTestUtility.row(
                "null", "baz",
                "foo", "foo",
                "foo-space-bar", "\"foo bar\"",
                "foo-newline-bar", "\"foo\nbar\"",
                "blank", "",
                "1space", "\" \"",
                "quoted-blank", "\"\"\"\"",
                "quoted-1space", "\"\" \"\""
        );

        assertEquals(expectedRow, actualRow);
    }
}
