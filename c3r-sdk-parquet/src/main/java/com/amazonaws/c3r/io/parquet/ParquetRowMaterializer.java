// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io.parquet;

import com.amazonaws.c3r.data.ParquetSchema;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.data.RowFactory;
import lombok.Getter;
import lombok.NonNull;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Takes raw Parquet data and turns it into usable Java values.
 */
public class ParquetRowMaterializer extends RecordMaterializer<Row<ParquetValue>> {
    /**
     * Creates a new row for Parquet values to be stored in.
     */
    private final RowFactory<ParquetValue> rowFactory;

    /**
     * Converts raw data into Java values.
     */
    private final ParquetGroupConverter root;

    /**
     * Set up a converter for a given schema specification along with a row generator for Parquet data.
     *
     * @param schema     Description of how data maps to columns, including associated metadata for each type
     * @param rowFactory Generate new empty rows to store Parquet data in
     */
    public ParquetRowMaterializer(final ParquetSchema schema, final RowFactory<ParquetValue> rowFactory) {
        this.rowFactory = rowFactory;
        root = new ParquetGroupConverter(schema, rowFactory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Row<ParquetValue> getCurrentRecord() {
        return root.getRow();
    }

    /**
     * Converter for Parquet data into values.
     *
     * @return Top level converter for transforming Parquet data into Java objects
     */
    @Override
    public GroupConverter getRootConverter() {
        return root;
    }

    /**
     * Converts raw Parquet primitive types into Java objects.
     */
    private static class ParquetGroupConverter extends GroupConverter {
        /**
         * Converters for each supported primitive data type.
         */
        private final List<ParquetPrimitiveConverter> converters;

        /**
         * Creates a new, empty row for Parquet values.
         */
        private final RowFactory<ParquetValue> rowFactory;

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
         * @param rowFactory Creates a new, empty row for Parquet values
         */
        ParquetGroupConverter(@NonNull final ParquetSchema schema, @NonNull final RowFactory<ParquetValue> rowFactory) {
            this.schema = schema;
            this.rowFactory = rowFactory;
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
            row = rowFactory.newRow();
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
}