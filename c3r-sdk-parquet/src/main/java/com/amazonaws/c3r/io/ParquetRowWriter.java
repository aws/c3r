// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.ParquetDataType;
import com.amazonaws.c3r.data.ParquetSchema;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.io.parquet.ParquetWriterBuilder;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.util.Collection;

/**
 * Resource for writing Parquet entries to an underlying store for processing.
 */
public final class ParquetRowWriter implements RowWriter<ParquetValue> {
    /**
     * Where to write Parquet data.
     */
    @Getter
    private final String targetName;

    /**
     * Interprets and writes Parquet data to target.
     */
    private final ParquetWriter<Row<ParquetValue>> rowWriter;

    /**
     * Description of the data type in each column along with Parquet metadata information.
     *
     * @see ParquetDataType
     */
    private final ParquetSchema parquetSchema;

    /**
     * Initialize a writer to put Parquet data of the specified types into the target file.
     *
     * @param targetName    Name of file to write to
     * @param parquetSchema Description of data types and metadata
     */
    @Builder
    private ParquetRowWriter(
            @NonNull final String targetName,
            @NonNull final ParquetSchema parquetSchema) {
        this.targetName = targetName;
        this.parquetSchema = parquetSchema;

        final org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(targetName);
        rowWriter = new ParquetWriterBuilder(file)
                .withSchema(parquetSchema)
                .build();
    }

    /**
     * Gets the headers for the output file.
     *
     * @return The ColumnHeaders of the output file
     */
    @Override
    public Collection<ColumnHeader> getHeaders() {
        return parquetSchema.getHeaders();
    }

    /**
     * Write a record to the store.
     *
     * @param row Row to write with columns mapped to respective values
     */
    @Override
    public void writeRow(@NonNull final Row<ParquetValue> row) {
        try {
            rowWriter.write(row);
        } catch (IOException e) {
            throw new C3rRuntimeException("Error writing to file " + targetName + ".", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        try {
            rowWriter.close();
        } catch (IOException e) {
            throw new C3rRuntimeException("Unable to close connection to Parquet file.", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush() {
        // N/A
    }
}
