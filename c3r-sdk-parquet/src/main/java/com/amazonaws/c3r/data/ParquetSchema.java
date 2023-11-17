// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

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
    private final org.apache.parquet.schema.MessageType reconstructedMessageType;

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
     * @deprecated Use the {@link ParquetSchema#builder()} method for this class.
     */
    @Deprecated
    public ParquetSchema(final org.apache.parquet.schema.MessageType messageType) {
        this(messageType, false, false);
    }

    /**
     * Generate a C3R schema based off of a Parquet schema.
     *
     * @param messageType Apache's Parquet schema
     * @param skipHeaderNormalization Whether headers should be normalized
     * @param binaryAsString If {@code true}, treat unannounced binary values as strings
     */
    @Builder
    private ParquetSchema(final org.apache.parquet.schema.MessageType messageType,
                          final boolean skipHeaderNormalization,
                          final Boolean binaryAsString) {
        reconstructedMessageType = reconstructMessageType(messageType, binaryAsString);
        headers = new ArrayList<>();
        columnIndices = new HashMap<>();

        final var parquetTypes = new HashMap<ColumnHeader, ParquetDataType>();
        final var clientTypes = new ArrayList<ClientDataType>(reconstructedMessageType.getFieldCount());
        for (int i = 0; i < reconstructedMessageType.getFieldCount(); i++) {
            final ColumnHeader column = skipHeaderNormalization
                    ? ColumnHeader.ofRaw(reconstructedMessageType.getFieldName(i))
                    : new ColumnHeader(reconstructedMessageType.getFieldName(i));
            headers.add(column);
            columnIndices.put(column, i);
            final org.apache.parquet.schema.Type originalType = reconstructedMessageType.getType(i);

            final ParquetDataType parquetType = ParquetDataType.fromType(originalType);
            parquetTypes.put(column, parquetType);
            clientTypes.add(parquetType.getClientDataType());
        }
        columnParquetDataTypeMap = Collections.unmodifiableMap(parquetTypes);
        columnClientDataTypes = Collections.unmodifiableList(clientTypes);
    }

    /**
     * If needed, examine all input Parquet types and reconstruct them according to the specifications.
     *
     * @param messageType    All types in the table and table name
     * @param binaryAsString If {@code true}, treat unannounced binary values as strings
     * @return Parquet types to use for marshalling
     */
    private MessageType reconstructMessageType(final MessageType messageType, final Boolean binaryAsString) {
        if (binaryAsString == null || !binaryAsString) {
            return new MessageType(messageType.getName(), messageType.getFields());
        }

        final List<Type> reconstructedFields = messageType.getFields().stream().map(this::reconstructType).collect(Collectors.toList());
        return new MessageType(messageType.getName(), reconstructedFields);
    }

    /**
     * Reconstruct Parquet types as needed. Currently only examines binary values,
     * adding the string annotation if needed as this is commonly left off.
     *
     * @param type Parquet type information to modify if needed
     * @return Finalized Parquet type information
     */
    private Type reconstructType(final Type type) {
        if (type.isPrimitive() && type.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY) {
            if (type.getLogicalTypeAnnotation() == null) {
                final Types.PrimitiveBuilder<PrimitiveType> construct;
                if (type.getRepetition() == Type.Repetition.OPTIONAL) {
                    construct = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY);
                } else if (type.getRepetition() == Type.Repetition.REQUIRED) {
                    construct = Types.required(PrimitiveType.PrimitiveTypeName.BINARY);
                } else {
                    construct = Types.repeated(PrimitiveType.PrimitiveTypeName.BINARY);
                }
                return construct.as(LogicalTypeAnnotation.stringType()).named(type.getName());
            }
        }
        return type;
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
                .map(c -> targetColumnParquetDataType(columnParquetDataTypeMap.get(c.getSourceHeader()), c.getType())
                        .toTypeWithName(c.getTargetHeader().toString()))
                .collect(Collectors.toList());

        return new ParquetSchema(new org.apache.parquet.schema.MessageType("EncryptedTable", fields));
    }

    /**
     * Check if target column type does not match input column type and set the target type.
     *
     * @param type       Input Parquet data type
     * @param columnType Targeted column type
     * @return Required type for output column
     */
    private ParquetDataType targetColumnParquetDataType(@NonNull final ParquetDataType type, @NonNull final ColumnType columnType) {
        if (type.getClientDataType() == ClientDataType.STRING) {
            return type;
        }

        if (columnType == ColumnType.FINGERPRINT || columnType == ColumnType.SEALED) {
            return ParquetDataType.fromType(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                    .named(type.getParquetType().getName()));
        }

        return type;
    }
}