// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.csv;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.util.Map;

/**
 * An implementation of BatchWrite that defines how to write the data to data source for batch processing.
 */
@AllArgsConstructor
public class CsvBatchWrite implements BatchWrite {

    /**
     * A map of configuration settings.
     */
    private final Map<String, String> properties;

    /**
     * {@inheritDoc}
     */
    @Override
    public DataWriterFactory createBatchWriterFactory(final PhysicalWriteInfo info) {
        return new CsvDataWriterFactory(properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit(final WriterCommitMessage[] messages) {
        // no-op
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void abort(final WriterCommitMessage[] messages) {
        // no-op
    }
}
