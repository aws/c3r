// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.MappedTableSchema;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ParquetSchemaTest {

    @Test
    public void emptyParquetSchemaTest() {
        final var emptySchema = new ParquetSchema(new MessageType("Empty", List.of()));
        assertEquals(0, emptySchema.size());
        assertEquals(0, emptySchema.getReconstructedMessageType().getColumns().size());
        assertEquals(0, emptySchema.getHeaders().size());
        assertEquals(0, emptySchema.getColumnParquetDataTypeMap().size());
    }

    @Test
    public void nonEmptyParquetSchemaTest() {
        final var messageType = new MessageType("Empty", List.of(
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .named("int32"),
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("string")
        ));
        final var nonEmptySchema = new ParquetSchema(messageType);
        assertEquals(2, nonEmptySchema.size());
        assertEquals(2, nonEmptySchema.getReconstructedMessageType().getColumns().size());
        assertEquals(
                List.of(new ColumnHeader("int32"), new ColumnHeader("string")),
                nonEmptySchema.getHeaders());
        assertEquals(
                Map.of(
                        new ColumnHeader("int32"),
                        ParquetDataType.fromType(messageType.getType(0)),

                        new ColumnHeader("string"),
                        ParquetDataType.fromType(messageType.getType(1))),
                nonEmptySchema.getColumnParquetDataTypeMap());
    }

    @Test
    public void parquetSchemaToEmptyTest() {
        final var messageType = new MessageType("Empty", List.of(
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .named("int32"),
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("string")
        ));
        final var derivedNonEmptySchema = new ParquetSchema(messageType)
                .deriveTargetSchema(new MappedTableSchema(List.of(
                        ColumnSchema.builder()
                                .sourceHeader(new ColumnHeader("int32"))
                                .type(ColumnType.CLEARTEXT)
                                .build()
                )));
        assertEquals(1, derivedNonEmptySchema.size());
        assertEquals(1, derivedNonEmptySchema.getReconstructedMessageType().getColumns().size());
        assertEquals(1, derivedNonEmptySchema.getHeaders().size());
        assertEquals(1, derivedNonEmptySchema.getColumnParquetDataTypeMap().size());
    }

    @Test
    public void parquetSchemaToIncompatibleSchemaTest() {
        final var parquetSchema = new ParquetSchema(new MessageType("Empty", List.of(
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .named("int32"),
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("string")
        )));

        assertThrows(C3rIllegalArgumentException.class, () ->
                parquetSchema.deriveTargetSchema(new MappedTableSchema(List.of(
                        ColumnSchema.builder()
                                .sourceHeader(new ColumnHeader("oops_this_header_does_not_exist"))
                                .type(ColumnType.CLEARTEXT)
                                .build()
                ))));
    }

    @Test
    public void getHeadersTest() {
        final var messageType = new MessageType("NameAndAge", List.of(
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("Name"),
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .named("Age")
        ));
        final var schema = ParquetSchema.builder().messageType(messageType).build();
        assertEquals(
                List.of(new ColumnHeader("Name"), new ColumnHeader("Age")),
                schema.getHeaders());
    }

    @Test
    public void getHeadersWithNormalizationTest() {
        final var messageType = new MessageType("NameAndAge", List.of(
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("Name"),
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .named("Age")
        ));
        final var schema = ParquetSchema.builder().messageType(messageType).skipHeaderNormalization(false).build();
        assertEquals(
                List.of(new ColumnHeader("name"), new ColumnHeader("age")),
                schema.getHeaders());
    }

    @Test
    public void getHeadersWithoutNormalizationTest() {
        final var messageType = new MessageType("NameAndAge", List.of(
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("Name"),
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .named("Age")
        ));
        final var schema = ParquetSchema.builder().messageType(messageType).skipHeaderNormalization(true).build();
        assertEquals(
                List.of(ColumnHeader.ofRaw("Name"), ColumnHeader.ofRaw("Age")),
                schema.getHeaders());
    }
}
