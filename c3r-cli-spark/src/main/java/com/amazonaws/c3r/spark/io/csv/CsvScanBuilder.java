// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.csv;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/**
 * An implementation of ScanBuilder for building the Scan.
 */
@AllArgsConstructor
public class CsvScanBuilder implements ScanBuilder {

    /**
     * A schema representation of the CSV file.
     */
    private final StructType schema;

    /**
     * A map of configuration settings.
     */
    private final Map<String, String> properties;

    /**
     * {@inheritDoc}
     */
    @Override
    public Scan build() {
        return new CsvScan(schema, properties);
    }
}
