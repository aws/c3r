// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.utils;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.ClientDataType;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.io.ParquetRowReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Map.entry;

/**
 * Utility class for testing Parquet functionality.
 */
public final class ParquetTestUtility {

    public static final String PARQUET_SAMPLE_DATA_PATH = "../samples/parquet/data_sample.parquet";

    /**
     * Raw column names and types for {@link #PARQUET_SAMPLE_DATA_PATH}.
     */
    public static final List<Map.Entry<String, ClientDataType>> PARQUET_SAMPLE_DATA_ROW_TYPE_ENTRIES =
            List.of(entry("FirstName", ClientDataType.STRING),
                    entry("LastName", ClientDataType.STRING),
                    entry("Address", ClientDataType.STRING),
                    entry("City", ClientDataType.STRING),
                    entry("State", ClientDataType.STRING),
                    entry("PhoneNumber", ClientDataType.STRING),
                    entry("Title", ClientDataType.STRING),
                    entry("Level", ClientDataType.STRING),
                    entry("Notes", ClientDataType.STRING));

    /**
     * ColumnHeaders for {@link #PARQUET_SAMPLE_DATA_PATH}.
     */
    public static final List<ColumnHeader> PARQUET_SAMPLE_DATA_HEADERS =
            PARQUET_SAMPLE_DATA_ROW_TYPE_ENTRIES.stream()
                    .map(Map.Entry::getKey)
                    .map(ColumnHeader::new)
                    .collect(Collectors.toList());

    /**
     * Non-normalized ColumnHeaders for {@link #PARQUET_SAMPLE_DATA_PATH}.
     */
    public static final List<ColumnHeader> PARQUET_SAMPLE_DATA_HEADERS_NO_NORMALIZATION =
            PARQUET_SAMPLE_DATA_ROW_TYPE_ENTRIES.stream()
                    .map(Map.Entry::getKey)
                    .map(ColumnHeader::ofRaw)
                    .collect(Collectors.toList());

    /**
     * A file containing a single row and single column of Parquet data.
     */
    public static final String PARQUET_1_ROW_PRIM_DATA_PATH = "../samples/parquet/rows_1_groups_1_prim_data.parquet";

    /**
     * A file containing 100 rows of a single column of Parquet data.
     */
    public static final String PARQUET_100_ROWS_PRIM_DATA_PATH = "../samples/parquet/rows_100_groups_1_prim_data.parquet";

    /**
     * A file containing 100 rows of 10 columns/groups of Parquet data.
     */
    public static final String PARQUET_100_ROWS_10_GROUPS_PRIM_DATA_PATH = "../samples/parquet/rows_100_groups_10_prim_data.parquet";

    /**
     * Parquet file that is not UTF-8 encoded.
     */
    public static final String PARQUET_NON_UTF8_DATA_PATH = "../samples/parquet/nonUtf8Encoding.parquet";

    /**
     * Parquet file with a single value that is null.
     */
    public static final String PARQUET_NULL_1_ROW_PRIM_DATA_PATH = "../samples/parquet/null_rows_1_groups_1_prim_data.parquet";

    /**
     * Parquet file with 100 rows of a single column that contains {@code null} as values.
     */
    public static final String PARQUET_NULL_100_ROWS_PRIM_DATA_PATH = "../samples/parquet/null_rows_100_groups_1_prim_data.parquet";

    /**
     * List of map entries of Parquet data type as column header to C3R data type.
     *
     * @see #PARQUET_TEST_DATA_TYPES
     */
    public static final List<Map.Entry<ColumnHeader, ClientDataType>> PARQUET_TEST_ROW_TYPE_ENTRIES =
            List.of(entry(new ColumnHeader("boolean"), ClientDataType.UNKNOWN),
                    entry(new ColumnHeader("string"), ClientDataType.STRING),
                    entry(new ColumnHeader("int8"), ClientDataType.UNKNOWN),
                    entry(new ColumnHeader("int16"), ClientDataType.UNKNOWN),
                    entry(new ColumnHeader("int32"), ClientDataType.UNKNOWN),
                    entry(new ColumnHeader("int64"), ClientDataType.UNKNOWN),
                    entry(new ColumnHeader("float"), ClientDataType.UNKNOWN),
                    entry(new ColumnHeader("double"), ClientDataType.UNKNOWN),
                    entry(new ColumnHeader("timestamp"), ClientDataType.UNKNOWN));

    /**
     * Map of Parquet data types as column headers to C3R data type.
     */
    public static final Map<ColumnHeader, ClientDataType> PARQUET_TEST_DATA_TYPES =
            PARQUET_TEST_ROW_TYPE_ENTRIES.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    /**
     * Parquet data types as column headers.
     *
     * @see #PARQUET_TEST_DATA_TYPES
     */
    public static final List<ColumnHeader> PARQUET_TEST_DATA_HEADERS =
            PARQUET_TEST_ROW_TYPE_ENTRIES.stream().map(Map.Entry::getKey).collect(Collectors.toList());

    /**
     * Hidden utility class constructor.
     */
    private ParquetTestUtility() {
    }

    /**
     * Read all rows from a Parquet file.
     *
     * @param filePath Location of file to read
     * @return List of individual rows of Parquet data
     * @throws IOException If the file could not be opened for reading
     */
    public static List<Row<ParquetValue>> readAllRows(final String filePath) {
        return readAllRows(new ParquetRowReader(filePath));
    }

    /**
     * Read all rows from a Parquet file using a {@link ParquetRowReader}.
     *
     * @param reader Reader configured for a specific Parquet file and the data types it contains
     * @return List of individual rows of Parquet data
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
