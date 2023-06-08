// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.csv;

import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A mix-in implementation of Table, to indicate that it's readable and writeable.
 */
@Accessors(fluent = true)
@Getter
public class CsvTable implements SupportsRead, SupportsWrite {

    /**
     * A schema representation of the CSV file.
     */
    private final StructType schema;

    /**
     * A map of configuration settings.
     */
    private final Map<String, String> properties;

    /**
     * A set of capabilities this Table supports.
     */
    private final Set<TableCapability> capabilities;

    /**
     * Constructs a new CsvTable.
     *
     * @param schema     the schema of the CSV file if not being inferred
     * @param properties A map of configuration settings
     */
    public CsvTable(final StructType schema, final Map<String, String> properties) {
        this.properties = new HashMap<>(properties);
        this.capabilities = new HashSet<>();
        capabilities.add(TableCapability.BATCH_READ);
        capabilities.add(TableCapability.BATCH_WRITE);

        this.schema = schema == null ? SchemaUtil.inferSchema(this.properties) : schema;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ScanBuilder newScanBuilder(final CaseInsensitiveStringMap options) {
        return new CsvScanBuilder(schema, properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return this.getClass().toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public WriteBuilder newWriteBuilder(final LogicalWriteInfo info) {
        return new CsvWriteBuilder(properties);
    }
}
