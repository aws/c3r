// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io.parquet;

import com.amazonaws.c3r.data.ParquetSchema;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;

import java.io.IOException;

/**
 * Builds a writer for Parquet data.
 */
public class ParquetWriterBuilder extends ParquetWriter.Builder<Row<ParquetValue>, ParquetWriterBuilder> {
    /**
     * Description of parquet data and how it maps to columns.
     */
    private ParquetSchema schema = null;

    /**
     * Sets up a writer for putting data in the specified file. Will overwrite file if it already exists.
     *
     * @param file Location of the file to write to
     */
    public ParquetWriterBuilder(final org.apache.hadoop.fs.Path file) {
        super(file);
        super.withWriteMode(ParquetFileWriter.Mode.OVERWRITE);
    }

    /**
     * Associate a schema describing data with the file.
     *
     * @param schema Description of parquet data and how it maps to columns
     * @return Parquet data writer with information about the data its writing
     */
    public ParquetWriterBuilder withSchema(final ParquetSchema schema) {
        this.schema = schema;
        return this;
    }

    /**
     * Override the Parquet {@code build} method to trap any I/O errors.
     *
     * @return A file writer for Parquet files with the specified schema
     * @throws C3rRuntimeException If there's an error while creating the file reader
     */
    @Override
    public ParquetWriter<Row<ParquetValue>> build() {
        try {
            return super.build();
        } catch (IOException e) {
            throw new C3rRuntimeException("Error while creating Parquet file reader.", e);
        }
    }

    /**
     * Creates a writer to be used for Parquet data output.
     *
     * @return Created writer will all needed information about the data
     */
    @Override
    protected ParquetWriterBuilder self() {
        return this;
    }

    /**
     * Create a {@link ParquetRowWriteSupport} instance with the specified schema and system configuration information.
     * (System configuration info currently is not needed.)
     *
     * @param conf System configuration information
     * @return Object to write Parquet data out to disc
     */
    @Override
    protected WriteSupport<Row<ParquetValue>> getWriteSupport(final Configuration conf) {
        return new ParquetRowWriteSupport(schema);
    }
}