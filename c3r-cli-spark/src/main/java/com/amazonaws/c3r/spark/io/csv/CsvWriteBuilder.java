// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.csv;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;

import java.util.Map;

/**
 * An implementation of WriterBuilder for building the Write.
 */
@AllArgsConstructor
public class CsvWriteBuilder implements WriteBuilder {

    /**
     * A map of configuration settings.
     */
    private final Map<String, String> properties;

    /**
     * {@inheritDoc}
     */
    @Override
    public Write build() {
        return new CsvWrite(properties);
    }
}
