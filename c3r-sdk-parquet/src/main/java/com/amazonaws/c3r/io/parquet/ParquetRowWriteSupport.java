// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io.parquet;

import com.amazonaws.c3r.data.ParquetSchema;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.data.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;

import java.util.HashMap;

/**
 * Convert Java values to raw Parquet primitive equivalents for writing.
 */
class ParquetRowWriteSupport extends WriteSupport<Row<ParquetValue>> {
    /**
     * Description of the data types plus metadata and which columns they map to.
     */
    private final ParquetSchema schema;

    /**
     * Writes Parquet data.
     */
    private RecordConsumer consumer;

    /**
     * Create object for writing Parquet data.
     *
     * @param schema Description of parquet data and how it maps to columns
     */
    ParquetRowWriteSupport(final ParquetSchema schema) {
        this.schema = schema;
    }

    /**
     * Call first to configure Parquet data writer.
     *
     * @param configuration the job's configuration
     * @return Information to write to file
     */
    @Override
    public WriteContext init(final Configuration configuration) {
        return new WriteContext(schema.getMessageType(), new HashMap<>());
    }

    /**
     * Prepare to write a new row of Parquet data to file.
     *
     * @param recordConsumer the recordConsumer to write to
     */
    @Override
    public void prepareForWrite(final RecordConsumer recordConsumer) {
        consumer = recordConsumer;
    }

    /**
     * Write a single record to the consumer specified by {@link #prepareForWrite}.
     *
     * @param row One row of Parquet data to write
     */
    @Override
    public void write(final Row<ParquetValue> row) {
        consumer.startMessage();
        writeRowContent(row);
        consumer.endMessage();
    }

    /**
     * Write the content for each value in a row.
     *
     * @param row Row of Parquet data to write
     */
    private void writeRowContent(final Row<ParquetValue> row) {
        row.forEach((column, value) -> {
            if (!value.isNull()) {
                final int i = schema.getColumnIndex(column);
                consumer.startField(column.toString(), i);
                value.writeValue(consumer);
                consumer.endField(column.toString(), i);
            }
        });
    }
}