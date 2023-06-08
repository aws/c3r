// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.csv;

import org.apache.spark.sql.connector.read.InputPartition;

/**
 * A serializable representation of an input partition returned by {@code Batch.planInputPartitions()} and the corresponding ones in
 * streaming.
 *
 * <p>
 * Note that InputPartition will be serialized and sent to executors, then PartitionReader will be created by {@code PartitionReaderFactory
 * .createReader(InputPartition)} or {@code PartitionReaderFactory.createColumnarReader(InputPartition)} on executors to do the actual
 * reading. So InputPartition must be serializable while PartitionReader doesn't need to be.
 */
public class CsvInputPartition implements InputPartition {

}
