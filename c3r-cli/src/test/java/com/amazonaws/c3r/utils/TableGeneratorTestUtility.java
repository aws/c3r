// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.utils;

import com.amazonaws.c3r.config.ColumnType;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import lombok.Builder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Used to generate CSV files with random data and an associated schema for testing purposes.
 */
@Builder
public final class TableGeneratorTestUtility {
    /**
     * Number of column types currently supported.
     */
    private static final int COL_TYPES = ColumnType.values().length;

    /**
     * Hidden utility class constructor.
     */
    private TableGeneratorTestUtility() {
    }

    /**
     * Stores associated schema and CSV files.
     */
    public static class Paths {
        /**
         * Location of schema file.
         */
        public final Path schema;

        /**
         * Location of CSV file.
         */
        public final Path data;

        /**
         * Stores paths to associated schema and CSV files.
         *
         * @param schema Location of schema file
         * @param data   Location of CSV file
         */
        public Paths(final Path schema, final Path data) {
            this.schema = schema;
            this.data = data;
        }
    }

    /**
     * Generates unique column header names based on type.
     *
     * @param columnIndex Which column to create a header for
     * @return Column type name followed by column number
     */
    private static String headerName(final int columnIndex) {
        switch (columnIndex % COL_TYPES) {
            case 0:
                return "cleartext" + columnIndex;
            case 1:
                return "sealed" + columnIndex;
            default:
                return "fingerprint" + columnIndex;
        }
    }

    /**
     * Generates the JSON output for a column schema. During data generation the column types are evenly rotated between:
     * <ul>
     *     <li>Cleartext</li>
     *     <li>Sealed with a Max Pad of Length 0</li>
     *     <li>Fingerprint</li>
     * </ul>
     *
     * @param columnIndex Which column to generate a schema for (determines types)
     * @return JSON object representing the column's schema
     */
    private static JsonObject columnSchema(final int columnIndex) {
        final JsonObject obj = new JsonObject();
        final JsonObject pad = new JsonObject();
        obj.addProperty("sourceHeader", headerName(columnIndex));
        switch (columnIndex % COL_TYPES) {
            case 0:
                obj.addProperty("type", "cleartext");
                break;
            case 1:
                obj.addProperty("type", "sealed");
                pad.addProperty("type", "max");
                pad.addProperty("length", 0);
                obj.add("pad", pad);
                break;
            default:
                obj.addProperty("type", "fingerprint");
                break;
        }

        return obj;
    }

    /**
     * Generates a prefix for the CSV and schema files.
     *
     * @param columnCount Number of columns in generated file
     * @param rowCount    Number of rows in generated file
     * @return String value {@code misc<columnCount>by<rowCount>-} for start of file name
     */
    public static String filePrefix(final int columnCount, final long rowCount) {
        return "misc" + columnCount + "by" + rowCount + "-";
    }

    /**
     * Generates a schema to match the generated CSV file. Column types rotate as specified in {@link #columnSchema(int)}.
     *
     * @param columnCount Number of columns in generated file
     * @param rowCount    Number of rows in generated file (used for naming file only)
     * @param directory   Where to write the schema file
     * @return Path to schema file
     * @throws IOException If there was an error writing the schema to disk
     */
    private static Path generateSchema(final int columnCount, final long rowCount, final Path directory) throws IOException {
        final JsonArray columns = new JsonArray(columnCount);
        for (int i = 0; i < columnCount; i++) {
            columns.add(columnSchema(i));
        }
        final JsonObject content = new JsonObject();
        content.add("headerRow", new JsonPrimitive(true));
        content.add("columns", columns);
        final Path path = directory.resolve(filePrefix(columnCount, rowCount) + ".json");
        final var writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
        writer.write(content.toString());
        writer.close();
        return path;
    }

    /**
     * Generate a random alphanumeric string of the specified size.
     *
     * @param size Number of characters in the string
     * @return Random alphanumeric string
     */
    private static String randomString(final int size) {
        final String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        final Random random = new Random();
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append(chars.charAt(random.nextInt(chars.length())));
        }
        return sb.toString();
    }

    /**
     * Creates a CSV file of the specified size filled with random alphanumeric strings.
     *
     * @param entrySize   Number of characters in each entry
     * @param columnCount Number of columns in the output file
     * @param rowCount    Number of rows in te output file
     * @param directory   Where to write the CSV file
     * @return Path to the generated file
     * @throws IOException If an error occurred while writing the file
     */
    private static Path generateCsv(final int entrySize, final int columnCount, final long rowCount, final Path directory)
            throws IOException {
        final Path path = directory.resolve(filePrefix(columnCount, rowCount) + ".csv");
        final var writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
        final var headers = IntStream.range(0, columnCount).boxed().map(TableGeneratorTestUtility::headerName)
                .collect(Collectors.joining(","));
        writer.write(headers);
        writer.write(System.lineSeparator());

        for (int i = 0; i < rowCount; i++) {
            final String entry = randomString(entrySize);
            final var entries = new String[columnCount];
            Arrays.fill(entries, entry);
            writer.write(String.join(",", entries));
            writer.write(System.lineSeparator());
        }
        writer.close();
        return path;
    }

    /**
     * Generate a schema and file for use in tests.
     *
     * @param entrySize   How long each individual entry should be
     * @param columnCount How many columns should be in the file
     * @param rowCount    How many rows should be in the file
     * @param directory   Where to put the file
     * @return Paths to the schema file and the data file
     * @throws IOException If there's an error writing to disk
     */
    public static Paths generateTestData(
            final int entrySize,
            final int columnCount,
            final long rowCount,
            final Path directory) throws IOException {
        return new Paths(
                generateSchema(columnCount, rowCount, directory),
                generateCsv(entrySize, columnCount, rowCount, directory));
    }
}
