// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.io.csv;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.io.CsvRowReader;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for creating schemas from ColumnHeaders.
 */
public abstract class SchemaUtil {

    /**
     * Create a schema based on the headers provided.
     *
     * @param headers The headers used to create a schema
     * @return a schema for the headers provided
     */
    public static StructType inferSchema(final List<ColumnHeader> headers) {
        final StructField[] fields = headers.stream().map(ColumnHeader::toString)
                .map(header -> DataTypes.createStructField(header, DataTypes.StringType, true))
                .toArray(StructField[]::new);
        return DataTypes.createStructType(fields);
    }

    /**
     * Create a schema based on the headers provided. If no headers were provided, attempt to read the headers.
     *
     * @param properties A map of configuration settings
     * @return a schema for the headers provided
     */
    public static StructType inferSchema(final Map<String, String> properties) {
        if (!properties.containsKey("headers")) {
            final CsvRowReader reader = SparkCsvReader.initReader(properties);
            return SchemaUtil.inferSchema(reader.getHeaders());
        }
        final String[] headers = properties.get("headers").split(",");
        final List<StructField> fields = Arrays.stream(headers)
                .map(field -> DataTypes.createStructField(field, DataTypes.StringType, true))
                .collect(Collectors.toList());
        return DataTypes.createStructType(fields);
    }
}
