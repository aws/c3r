// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.csv;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.CsvRow;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.io.CsvRowWriter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A data writer returned by {@code DataWriterFactory.createWriter(int, long)} and is responsible for writing data for an input RDD
 * partition.
 */
public class CsvDataWriter implements DataWriter<InternalRow> {

    /**
     * Writer for writing CSV files.
     */
    private final CsvRowWriter writer;

    /**
     * Constructs a new CsvPartitionReader.
     *
     * @param partitionId The partition being processed
     * @param properties  A map of configuration settings
     */
    public CsvDataWriter(final int partitionId, final Map<String, String> properties) {
        this.writer = SparkCsvWriter.initWriter(partitionId, properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(final InternalRow record) throws IOException {
        final Row<CsvValue> toWrite = new CsvRow();
        final List<ColumnHeader> columns = writer.getHeaders();
        if (record.numFields() != columns.size()) {
            throw new C3rRuntimeException("Column count mismatch when writing row. Expected "
                    + columns.size() + " but was " + record.numFields());
        }
        for (int i = 0; i < record.numFields(); i++) {
            final CsvValue value;
            if (record.getUTF8String(i) == null) { // Null UTF8String can't be converted to String
                value = new CsvValue((String) null);
            } else {
                value = new CsvValue(record.getString(i));
            }
            toWrite.putValue(columns.get(i), value);
        }
        writer.writeRow(toWrite);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public WriterCommitMessage commit() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void abort() {
        // no-op
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        writer.close();
    }
}
