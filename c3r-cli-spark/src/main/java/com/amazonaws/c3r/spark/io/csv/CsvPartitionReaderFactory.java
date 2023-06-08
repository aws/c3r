// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.csv;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

import java.util.Map;

/**
 * A factory used to create PartitionReader instances.
 *
 * <p>
 * If Spark fails to execute any methods in the implementations of this interface or in the returned PartitionReader (by throwing an
 * exception), corresponding Spark task would fail and get retried until hitting the maximum retry times.
 */
@AllArgsConstructor
public class CsvPartitionReaderFactory implements PartitionReaderFactory {

    /**
     * A map of configuration settings.
     */
    private final Map<String, String> properties;

    /**
     * {@inheritDoc}
     */
    @Override
    public PartitionReader<InternalRow> createReader(final InputPartition partition) {
        return new CsvPartitionReader(properties);
    }
}
