// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.csv;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

/**
 * Custom CSV DataSource for Spark. Using this custom DataSource in place of Spark's built-in functionality allows us to maintain tighter
 * controls of edge cases like {@code null}, quoted empty space, and custom null values.
 */
public class Csv implements TableProvider {

    /**
     * {@inheritDoc}
     */
    @Override
    public StructType inferSchema(final CaseInsensitiveStringMap options) {
        return SchemaUtil.inferSchema(options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Table getTable(final StructType schema, final Transform[] partitioning, final Map<String, String> properties) {
        return new CsvTable(schema, properties);
    }

}
