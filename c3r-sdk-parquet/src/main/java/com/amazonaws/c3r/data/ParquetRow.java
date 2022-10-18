// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.internal.Nonce;
import lombok.NonNull;

import java.util.Map;
import java.util.Objects;

/**
 * Row of Parquet data.
 */
public final class ParquetRow extends Row<ParquetValue> {
    /**
     * Maps column name to data type.
     */
    private final Map<ColumnHeader, ParquetDataType> columnTypes;

    /**
     * Stores the Parquet values for a particular row.
     *
     * @param columnTypes Maps column name to data type in column
     */
    public ParquetRow(final Map<ColumnHeader, ParquetDataType> columnTypes) {
        this.columnTypes = Map.copyOf(columnTypes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putBytes(@NonNull final ColumnHeader column, final byte[] bytes) {
        final ParquetDataType parquetType = Objects.requireNonNull(this.columnTypes.get(column),
                "Internal error! Unknown column: " + column);
        putValue(column, ParquetValue.fromBytes(parquetType, bytes));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putNonce(@NonNull final ColumnHeader nonceColumn, final Nonce nonce) {
        putValue(nonceColumn, ParquetValue.fromBytes(ParquetDataType.NONCE_TYPE, nonce.getBytes()));
    }

    @Override
    public Row<ParquetValue> clone() {
        final Row<ParquetValue> row = new ParquetRow(columnTypes);
        getHeaders().forEach(header -> row.putValue(header, getValue(header)));
        return row;
    }
}
