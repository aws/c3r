// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io.parquet;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.ParquetDataType;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

/**
 * Converts an individual Parquet entry/value, adding it to a specified row.
 */
class ParquetPrimitiveConverter extends PrimitiveConverter {
    /**
     * Name of the column.
     */
    @Getter
    private final ColumnHeader column;

    /**
     * Type of data stored in the column.
     */
    private final ParquetDataType columnType;

    /**
     * Specific row location to store value.
     */
    @Setter
    private Row<ParquetValue> row;

    /**
     * Construct a converter for a Parquet value associated with a particular
     * {@code column} and {@code row}.
     *
     * @param column     The column the value will be associated with
     * @param columnType Type of data in this column
     * @throws C3rRuntimeException if the Parquet type is a group
     */
    ParquetPrimitiveConverter(@NonNull final ColumnHeader column, @NonNull final ParquetDataType columnType) {
        if (!columnType.getParquetType().isPrimitive()) {
            throw new C3rRuntimeException("C3R only supports Parquet primitive types.");
        }
        this.column = column;
        this.columnType = columnType;
    }

    /**
     * Puts the Parquet value into the corresponding column entry for this row.
     *
     * @param value Parquet primitive value
     * @throws C3rRuntimeException if row does not exist or value in row is being overwritten
     */
    private void addValue(final ParquetValue value) {
        if (row == null) {
            throw new C3rRuntimeException("ParquetPrimitiveConverter called without a row to populate!");
        }
        if (row.hasColumn(column)) {
            throw new C3rRuntimeException("ParquetPrimitiveConverter is overwriting the column " + column + ".");
        }
        row.putValue(column, value);
    }

    /**
     * Puts the specified Parquet binary value into the corresponding column entry for this row.
     *
     * @param value Parquet binary value
     */
    @Override
    public void addBinary(final Binary value) {
        addValue(new ParquetValue.Binary(columnType, value));
    }

    /**
     * Puts the specified Parquet boolean value into the corresponding column entry for this row.
     *
     * @param value Parquet boolean value
     */
    @Override
    public void addBoolean(final boolean value) {
        addValue(new ParquetValue.Boolean(columnType, value));
    }

    /**
     * Puts the specified Parquet double value into the corresponding column entry for this row.
     *
     * @param value Parquet double value
     */
    @Override
    public void addDouble(final double value) {
        addValue(new ParquetValue.Double(columnType, value));
    }

    /**
     * Puts the specified Parquet float value into the corresponding column entry for this row.
     *
     * @param value Parquet float value
     */
    @Override
    public void addFloat(final float value) {
        addValue(new ParquetValue.Float(columnType, value));
    }

    /**
     * Puts the specified Parquet integer (int32) value into the corresponding column entry for this row.
     *
     * @param value Parquet integer value
     */
    @Override
    public void addInt(final int value) {
        addValue(new ParquetValue.Int32(columnType, value));
    }

    /**
     * Puts the specified Parquet long (int64) value into the corresponding column entry for this row.
     *
     * @param value Parquet long value
     */
    @Override
    public void addLong(final long value) {
        addValue(new ParquetValue.Int64(columnType, value));
    }
}