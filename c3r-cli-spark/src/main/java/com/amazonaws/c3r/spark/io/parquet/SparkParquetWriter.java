// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.parquet;

import lombok.NonNull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

/**
 * Utility class for Spark to write Parquet files to disk.
 */
public abstract class SparkParquetWriter {

    /**
     * Writes the Dataset to the root path.
     *
     * @param dataset    The data to write
     * @param targetName The target path to write to
     */
    public static void writeOutput(@NonNull final Dataset<Row> dataset,
                                   @NonNull final String targetName) {
        dataset.write().mode(SaveMode.Append)
                .parquet(targetName);
    }
}
