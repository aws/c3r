// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cli;

import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.io.CsvRowReader;
import com.amazonaws.c3r.utils.StringTestUtility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchemaModeTest {
    private static final int SAMPLE_DATA_COLUMN_COUNT = 9;

    private static final String MATCH_ALL_COLUMN_TYPES = "[" + ColumnType.CLEARTEXT + "|" + ColumnType.SEALED + "|"
            + ColumnType.FINGERPRINT + "]";

    private Path schemaPath;

    @BeforeEach
    public void setup() throws IOException {
        schemaPath = Files.createTempFile("data_sample", ".json");
        schemaPath.toFile().deleteOnExit();
    }

    @AfterEach
    public void teardown() throws IOException {
        Files.delete(schemaPath);
    }

    @Test
    public void schemaTemplateCsvTest() throws IOException {
        final String originalPath = "../samples/csv/data_sample_without_quotes.csv";

        final var args = SchemaCliConfigTestUtility.builder()
                .input(originalPath)
                .output(schemaPath.toString())
                .subMode("--template")
                .overwrite(true)
                .build();

        final int exitCode = SchemaMode.getApp().execute(args.toArrayWithoutMode());

        assertEquals(0, exitCode);

        assertTrue(Files.exists(schemaPath));
        assertTrue(Files.size(schemaPath) > 0);
        final String contents = Files.readString(schemaPath);
        assertTrue(contents.contains("\"headerRow\": true"));
        assertEquals(SAMPLE_DATA_COLUMN_COUNT,
                StringTestUtility.countMatches("sourceHeader", contents));
        assertEquals(SAMPLE_DATA_COLUMN_COUNT,
                StringTestUtility.countMatches(MATCH_ALL_COLUMN_TYPES, contents));
    }

    @Test
    public void schemaTemplateCsvNoHeadersTest() throws IOException {
        final String originalPath = "../samples/csv/data_sample_no_headers.csv";

        final var args = SchemaCliConfigTestUtility.builder()
                .input(originalPath)
                .output(schemaPath.toString())
                .subMode("--template")
                .overwrite(true)
                .noHeaders(true)
                .build();

        final int exitCode = SchemaMode.getApp().execute(args.toArrayWithoutMode());

        assertEquals(0, exitCode);

        assertTrue(Files.exists(schemaPath));
        assertTrue(Files.size(schemaPath) > 0);
        final String contents = Files.readString(schemaPath);
        assertTrue(contents.contains("\"headerRow\": false"));
        assertEquals(0,
                StringTestUtility.countMatches("sourceHeader", contents));
        assertEquals(SAMPLE_DATA_COLUMN_COUNT,
                StringTestUtility.countMatches(MATCH_ALL_COLUMN_TYPES, contents));
    }

    @Test
    public void schemaTemplateParquetTest() throws IOException {
        final String originalPath = "../samples/parquet/data_sample.parquet";

        final var args = SchemaCliConfigTestUtility.builder()
                .input(originalPath)
                .output(schemaPath.toString())
                .subMode("--template")
                .overwrite(true)
                .build();

        final int exitCode = SchemaMode.getApp().execute(args.toArrayWithoutMode());

        assertEquals(0, exitCode);

        assertTrue(Files.exists(schemaPath));
        assertTrue(Files.size(schemaPath) > 0);
        final String contents = Files.readString(schemaPath);
        assertTrue(contents.contains("\"headerRow\": true"));
        assertEquals(SAMPLE_DATA_COLUMN_COUNT,
                StringTestUtility.countMatches("sourceHeader", contents));
        assertEquals(SAMPLE_DATA_COLUMN_COUNT,
                StringTestUtility.countMatches(MATCH_ALL_COLUMN_TYPES, contents));
    }

    // Check that interactive schema command returns results and shallowly check content contains expected entries
    @Test
    public void schemaInteractiveCsvTest() throws IOException {
        final String originalPath = "../samples/csv/data_sample_without_quotes.csv";

        final var args = SchemaCliConfigTestUtility.builder()
                .input(originalPath)
                .output(schemaPath.toAbsolutePath().toString())
                .subMode("--interactive")
                .overwrite(true)
                .build();

        // user input which repeatedly says the source column in question should generate one cleartext column
        // with the default name
        final var userInput = new ByteArrayInputStream("1\ncleartext\n\n".repeat(100).getBytes(StandardCharsets.UTF_8));
        System.setIn(new BufferedInputStream(userInput));

        final int exitCode = Main.getApp().execute(args.toArray());
        assertEquals(0, exitCode);

        assertTrue(schemaPath.toFile().exists());
        assertTrue(schemaPath.toFile().length() > 0);
        final String contents = Files.readString(schemaPath);
        assertTrue(contents.contains("\"headerRow\": true"));
        assertEquals(SAMPLE_DATA_COLUMN_COUNT,
                StringTestUtility.countMatches("sourceHeader", contents));
        assertEquals(SAMPLE_DATA_COLUMN_COUNT,
                StringTestUtility.countMatches(ColumnType.CLEARTEXT.toString(), contents));
    }

    // Check that interactive schema command returns results and shallowly check content contains expected entries
    @Test
    public void schemaInteractiveCsvWithoutHeadersTest() throws IOException {
        final String originalPath = "../samples/csv/data_sample_no_headers.csv";

        final var args = SchemaCliConfigTestUtility.builder()
                .input(originalPath)
                .output(schemaPath.toAbsolutePath().toString())
                .subMode("--interactive")
                .noHeaders(true)
                .overwrite(true)
                .build();

        // user input which repeatedly says the source column in question should generate one cleartext column
        // with the default name
        final int columnCount = CsvRowReader.getCsvColumnCount(originalPath, StandardCharsets.UTF_8);
        final StringBuilder inputBuilder = new StringBuilder();
        for (int i = 0; i < columnCount; i++) {
            // 1 target column
            inputBuilder.append("1\n");
            // target column type
            inputBuilder.append("cleartext\n");
            // target column name
            inputBuilder.append("column").append(i).append('\n');
        }

        final var userInput = new ByteArrayInputStream(inputBuilder.toString().getBytes(StandardCharsets.UTF_8));
        System.setIn(new BufferedInputStream(userInput));

        final int exitCode = Main.getApp().execute(args.toArray());
        assertEquals(0, exitCode);

        assertTrue(schemaPath.toFile().exists());
        assertTrue(schemaPath.toFile().length() > 0);
        final String contents = Files.readString(schemaPath);
        assertTrue(contents.contains("\"headerRow\": false"));
        assertEquals(0,
                StringTestUtility.countMatches("sourceHeader", contents));
        assertEquals(SAMPLE_DATA_COLUMN_COUNT,
                StringTestUtility.countMatches(ColumnType.CLEARTEXT.toString(), contents));
    }
}
