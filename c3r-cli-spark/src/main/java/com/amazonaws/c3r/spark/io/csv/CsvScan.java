// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.csv;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/**
 * A logical representation of a data source scan. This interface is used to provide logical information, like what the actual read
 * schema is.
 */
@AllArgsConstructor
public class CsvScan implements Scan {

    /**
     * A schema representation of the CSV file.
     */
    @Getter
    @Accessors(fluent = true)
    private final StructType readSchema;

    /**
     * A map of configuration settings.
     */
    private final Map<String, String> properties;

    /**
     * {@inheritDoc}
     */
    @Override
    public Batch toBatch() {
        return new CsvBatch(properties);
    }
}
