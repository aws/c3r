// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.ParquetSchema;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.data.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utility functions for reading Parquet data out of files.
 */
public final class ParquetTestUtility {
    /**
     * Hidden utility class constructor.
     */
    private ParquetTestUtility() {
    }

    /**
     * Takes a row of Parquet values and returns them as an array of string values ordered by column indices.
     *
     * @param row     Parquet values looked up by name
     * @param indices Mapping of column index to name
     * @return Ordered Parquet values converted to strings
     */
    private static String[] rowToStringArray(final Row<ParquetValue> row, final Map<Integer, ColumnHeader> indices) {
        final String[] strings = new String[row.size()];
        for (int i = 0; i < row.size(); i++) {
            strings[i] = Objects.requireNonNullElse(row.getValue(indices.get(i)).toString(), "");
        }
        return strings;
    }

    /**
     * Reads a Parquet file into a list of ordered string values.
     *
     * @param filePath Location of the file to read
     * @return Contents of the file as a list of rows and the rows are string values
     */
    public static List<String[]> readContentAsStringArrays(final String filePath) {
        final ParquetRowReader reader = ParquetRowReader.builder().sourceName(filePath).build();
        final ParquetSchema parquetSchema = reader.getParquetSchema();
        final Map<Integer, ColumnHeader> columnIndices = parquetSchema.getHeaders().stream()
                .collect(Collectors.toMap(
                        parquetSchema::getColumnIndex,
                        Function.identity()
                ));
        final var mapRows = readAllRows(reader);
        return mapRows.stream().map(row -> rowToStringArray(row, columnIndices)).collect(Collectors.toList());
    }

    /**
     * Reads all the rows from a Parquet file to their Parquet type.
     *
     * @param reader Reads a particular Parquet file
     * @return Contents of the file as a list of rows with Parquet values
     */
    public static List<Row<ParquetValue>> readAllRows(final ParquetRowReader reader) {
        final var rows = new ArrayList<Row<ParquetValue>>();
        while (reader.hasNext()) {
            final var row = reader.next();
            rows.add(row);
        }
        return rows;
    }
}
