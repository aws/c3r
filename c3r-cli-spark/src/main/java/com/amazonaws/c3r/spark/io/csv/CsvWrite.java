// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.csv;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;

import java.util.Map;

/**
 * A logical representation of a data source write.
 */
@AllArgsConstructor
public class CsvWrite implements Write {

    /**
     * A map of configuration settings.
     */
    private final Map<String, String> properties;

    /**
     * {@inheritDoc}
     */
    @Override
    public BatchWrite toBatch() {
        return new CsvBatchWrite(properties);
    }
}
