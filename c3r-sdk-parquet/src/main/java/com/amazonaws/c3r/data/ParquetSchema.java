// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import lombok.Getter;
import org.apache.parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Schema for a source Parquet file.
 */
public class ParquetSchema {
    /**
     * Parquet schema.
     */
    @Getter
    private final org.apache.parquet.schema.MessageType messageType;

    /**
     * Column names.
     */
    @Getter
    private final List<ColumnHeader> headers;

    /**
     * Map of column names to column index.
     */
    private final Map<ColumnHeader, Integer> columnIndices;

    /**
     * Map of column names to Parquet data type.
     */
    @Getter
    private final Map<ColumnHeader, ParquetDataType> columnParquetDataTypeMap;

    /**
     * Client data types associated with the parquet file's columns.
     */
    @Getter
    private final List<ClientDataType> columnClientDataTypes;

    /**
     * Generate a C3R schema based off of a Parquet schema.
     *
     * @param messageType Apache's Parquet schema
     */
    public ParquetSchema(final org.apache.parquet.schema.MessageType messageType) {
        this.messageType = new MessageType(messageType.getName(), messageType.getFields());
        headers = new ArrayList<>();
        columnIndices = new HashMap<>();

        final var parquetTypes = new HashMap<ColumnHeader, ParquetDataType>();
        final var clientTypes = new ArrayList<ClientDataType>(messageType.getFieldCount());
        for (int i = 0; i < messageType.getFieldCount(); i++) {
            final ColumnHeader column = new ColumnHeader(messageType.getFieldName(i));
            headers.add(column);
            columnIndices.put(column, i);
            final org.apache.parquet.schema.Type originalType = messageType.getType(i);

            final ParquetDataType parquetType = ParquetDataType.fromType(originalType);
            parquetTypes.put(column, parquetType);
            clientTypes.add(parquetType.getClientDataType());
        }
        columnParquetDataTypeMap = Collections.unmodifiableMap(parquetTypes);
        columnClientDataTypes = Collections.unmodifiableList(clientTypes);
    }

    /**
     * How many columns are in the file.
     *
     * @return Column count
     */
    public int size() {
        return headers.size();
    }

    /**
     * Get the column index for the named column.
     *
     * @param column Name of the column
     * @return Indexed location of column
     */
    public int getColumnIndex(final ColumnHeader column) {
        return columnIndices.get(column);
    }

    /**
     * Get the Parquet type for a column.
     *
     * @param column Name of the column
     * @return Type of the column
     */
    public ParquetDataType getColumnType(final ColumnHeader column) {
        return columnParquetDataTypeMap.get(column);
    }

    /**
     * Constructs the target {@link ParquetSchema} using this as the source
     * {@link ParquetSchema} and the given {@link TableSchema}.
     *
     * @param tableSchema The table schema mapping this source {@link ParquetSchema} to
     *                    the corresponding target {@link ParquetSchema}. NOTE: each
     *                    `sourceHeader` in the schema must be present as a field in the
     *                    source {@link ParquetSchema} or an error is raised
     * @return The corresponding target schema described by this {@link ParquetSchema} and the given {@link TableSchema}
     * @throws C3rIllegalArgumentException If the source header is not found in schema
     */
    public ParquetSchema deriveTargetSchema(final TableSchema tableSchema) {
        for (ColumnSchema c : tableSchema.getColumns()) {
            if (!columnParquetDataTypeMap.containsKey(c.getSourceHeader())) {
                throw new C3rIllegalArgumentException("sourceHeader in schema not found in Parquet schema: "
                        + c.getSourceHeader());
            }
        }

        final List<org.apache.parquet.schema.Type> fields = tableSchema.getColumns().stream()
                .map(c -> columnParquetDataTypeMap.get(c.getSourceHeader()).toTypeWithName(c.getTargetHeader().toString()))
                .collect(Collectors.toList());

        return new ParquetSchema(new org.apache.parquet.schema.MessageType("EncryptedTable", fields));
    }
}