// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.config.ColumnHeader;

import java.util.Map;

/**
 * Factory for creating empty Parquet rows with the given column types.
 */
public class ParquetRowFactory implements RowFactory<ParquetValue> {
    /**
     * Map column name to type.
     */
    private final Map<ColumnHeader, ParquetDataType> columnTypes;

    /**
     * Initializes the factory with type information for the row.
     *
     * @param columnTypes Data type associated with a column name
     */
    public ParquetRowFactory(final Map<ColumnHeader, ParquetDataType> columnTypes) {
        this.columnTypes = Map.copyOf(columnTypes);
    }

    /**
     * Create an empty row to be populated by the callee.
     *
     * @return An empty Row for storing data in
     */
    @Override
    public Row<ParquetValue> newRow() {
        return new ParquetRow(columnTypes);
    }
}
