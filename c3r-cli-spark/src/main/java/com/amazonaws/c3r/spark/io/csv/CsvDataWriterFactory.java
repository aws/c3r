// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.csv;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;

import java.util.Map;

/**
 * A factory of DataWriter returned by {@code BatchWrite.createBatchWriterFactory(PhysicalWriteInfo)}, which is responsible for creating and
 * initializing the actual data writer at executor side.
 *
 * <p>
 * Note that, the writer factory will be serialized and sent to executors, then the data writer will be created on executors and do the
 * actual writing. So this interface must be serializable and DataWriter doesn't need to be.
 */
@AllArgsConstructor
public class CsvDataWriterFactory implements DataWriterFactory {
    /**
     * A map of configuration settings.
     */
    private final Map<String, String> properties;

    /**
     * {@inheritDoc}
     */
    @Override
    public DataWriter<InternalRow> createWriter(final int partitionId, final long taskId) {
        return new CsvDataWriter(partitionId, properties);
    }
}
