// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io.parquet;

import com.amazonaws.c3r.data.ParquetSchema;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.data.ValueFactory;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;

/**
 * Takes raw Parquet data and turns it into usable Java values.
 */
public class ParquetRowMaterializer extends RecordMaterializer<Row<ParquetValue>> {
    /**
     * Converts raw data into Java values.
     */
    private final ParquetRowConverter root;

    /**
     * Set up a converter for a given schema specification along with a row generator for Parquet data.
     *
     * @param schema     Description of how data maps to columns, including associated metadata for each type
     * @param valueFactory Generate new empty rows to store Parquet data in
     */
    public ParquetRowMaterializer(final ParquetSchema schema, final ValueFactory<ParquetValue> valueFactory) {
        // Creates a new row for Parquet values to be stored in
        root = new ParquetRowConverter(schema, valueFactory);
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
}