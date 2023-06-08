// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.csv;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

import java.util.Map;

/**
 * A physical representation of a data source scan for batch queries. This interface is used to provide physical information, like how
 * many partitions the scanned data has, and how to read records from the partitions.
 */
@AllArgsConstructor
public class CsvBatch implements Batch {

    /**
     * A map of configuration settings.
     */
    private final Map<String, String> properties;

    /**
     * {@inheritDoc}
     */
    @Override
    public InputPartition[] planInputPartitions() {
        return new InputPartition[]{new CsvInputPartition()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new CsvPartitionReaderFactory(properties);
    }
}
