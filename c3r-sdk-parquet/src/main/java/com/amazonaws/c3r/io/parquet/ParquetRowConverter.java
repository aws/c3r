// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io.parquet;

import com.amazonaws.c3r.data.ParquetSchema;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.data.ValueFactory;
import lombok.Getter;
import lombok.NonNull;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts raw Parquet primitive types into Java objects.
 */
public class ParquetRowConverter extends GroupConverter {
    /**
     * Converters for each supported primitive data type.
     */
    private final List<ParquetPrimitiveConverter> converters;

    /**
     * Creates a new, empty row for Parquet values.
     */
    private final ValueFactory<ParquetValue> valueFactory;

    /**
     * Description of the data types plus metadata and which columns they map to.
     */
    private final ParquetSchema schema;

    /**
     * Row of data currently being filled out.
     */
    @Getter
    private Row<ParquetValue> row;

    /**
     * Sets up for converting data from raw Parquet data into Java objects.
     *
     * @param schema     Description of the data types plus metadata and which columns they map to
     * @param valueFactory Creates a new, empty row for Parquet values
     */
    ParquetRowConverter(@NonNull final ParquetSchema schema, @NonNull final ValueFactory<ParquetValue> valueFactory) {
        this.schema = schema;
        this.valueFactory = valueFactory;
        converters = schema.getHeaders().stream()
                .map(c -> new ParquetPrimitiveConverter(c, schema.getColumnType(c)))
                .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Converter getConverter(final int fieldIndex) {
        return converters.get(fieldIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        row = valueFactory.newRow();
        for (var converter : converters) {
            converter.setRow(row);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void end() {
        if (row.size() != schema.getHeaders().size()) {
            // Fill in any missing entries with explicit NULLs
            for (var header : schema.getHeaders()) {
                if (!row.hasColumn(header)) {
                    row.putBytes(header, null);
                }
            }
        }
    }
}
