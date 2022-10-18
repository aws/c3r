// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io.parquet;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.ParquetDataType;
import com.amazonaws.c3r.data.ParquetRow;
import com.amazonaws.c3r.data.ParquetValue;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ParquetPrimitiveConverterTest {
    private static final Type INT32_TYPE = Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("int32");

    private static final Type INT64_TYPE = Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("int64");

    private static final Type BINARY_TYPE = Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named("binary");

    private static final Type BOOLEAN_TYPE = Types.required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("boolean");

    private static final Type FLOAT_TYPE = Types.required(PrimitiveType.PrimitiveTypeName.FLOAT).named("float");

    private static final Type DOUBLE_TYPE = Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("double");

    private static final ColumnHeader FOO_HEADER = new ColumnHeader("foo");

    private static final ColumnHeader BAR_HEADER = new ColumnHeader("bar");

    private ParquetRow row;

    private ParquetPrimitiveConverter fooConverter;

    private ParquetPrimitiveConverter barConverter;

    public void setup(final Type type) {
        row = new ParquetRow(Map.of(
                FOO_HEADER, ParquetDataType.fromType(type),
                BAR_HEADER, ParquetDataType.fromType(type)
        ));
        fooConverter = new ParquetPrimitiveConverter(FOO_HEADER, ParquetDataType.fromType(
                type
        ));
        barConverter = new ParquetPrimitiveConverter(BAR_HEADER, ParquetDataType.fromType(
                type
        ));
        fooConverter.setRow(row);
        barConverter.setRow(row);
    }

    @Test
    public void addMissingRowTest() {
        fooConverter = new ParquetPrimitiveConverter(FOO_HEADER, ParquetDataType.fromType(
                BINARY_TYPE
        ));
        assertThrows(C3rRuntimeException.class, () ->
                fooConverter.addBinary(Binary.fromReusedByteArray(new byte[]{0})));
    }

    @Test
    public void addRepeatValueTest() {
        row = new ParquetRow(Map.of(
                FOO_HEADER, ParquetDataType.fromType(BINARY_TYPE)
        ));
        fooConverter = new ParquetPrimitiveConverter(FOO_HEADER, ParquetDataType.fromType(
                BINARY_TYPE
        ));
        fooConverter.setRow(row);
        assertDoesNotThrow(() ->
                fooConverter.addBinary(Binary.fromReusedByteArray(new byte[]{0})));
        assertThrows(C3rRuntimeException.class, () ->
                fooConverter.addBinary(Binary.fromReusedByteArray(new byte[]{0})));
    }

    @Test
    public void addBinaryTest() {
        setup(BINARY_TYPE);

        fooConverter.addBinary(Binary.fromReusedByteArray(new byte[]{0}));
        assertArrayEquals(new byte[]{0}, row.getValue(FOO_HEADER).getBytes());

        barConverter.addBinary(Binary.fromReusedByteArray(new byte[]{1}));
        assertArrayEquals(new byte[]{1}, row.getValue(BAR_HEADER).getBytes());
    }

    @Test
    public void addBooleanTest() {
        setup(BOOLEAN_TYPE);
        fooConverter.addBoolean(true);
        assertEquals(
                new ParquetValue.Boolean(ParquetDataType.fromType(BOOLEAN_TYPE), true),
                row.getValue(FOO_HEADER));

        barConverter.addBoolean(false);
        assertEquals(
                new ParquetValue.Boolean(ParquetDataType.fromType(BOOLEAN_TYPE), false),
                row.getValue(BAR_HEADER));
    }

    @Test
    public void addFloatTest() {
        setup(FLOAT_TYPE);
        fooConverter.addFloat(3.14159f);
        assertEquals(
                new ParquetValue.Float(ParquetDataType.fromType(FLOAT_TYPE), 3.14159f),
                row.getValue(FOO_HEADER));

        barConverter.addFloat(42.0f);
        assertEquals(
                new ParquetValue.Float(ParquetDataType.fromType(FLOAT_TYPE), 42.0f),
                row.getValue(BAR_HEADER));
    }

    @Test
    public void addDoubleTest() {
        setup(DOUBLE_TYPE);
        fooConverter.addDouble(3.14159);
        assertEquals(
                new ParquetValue.Double(ParquetDataType.fromType(DOUBLE_TYPE), 3.14159),
                row.getValue(FOO_HEADER));

        barConverter.addDouble(42.0);
        assertEquals(
                new ParquetValue.Double(ParquetDataType.fromType(DOUBLE_TYPE), 42.0),
                row.getValue(BAR_HEADER));
    }

    @Test
    public void addIntTest() {
        setup(INT32_TYPE);
        fooConverter.addInt(3);
        assertEquals(
                new ParquetValue.Int(ParquetDataType.fromType(INT32_TYPE), 3),
                row.getValue(FOO_HEADER));

        barConverter.addInt(42);
        assertEquals(
                new ParquetValue.Int(ParquetDataType.fromType(INT32_TYPE), 42),
                row.getValue(BAR_HEADER));
    }

    @Test
    public void addLongTest() {
        setup(INT64_TYPE);
        fooConverter.addLong(3L);
        assertEquals(
                new ParquetValue.Long(ParquetDataType.fromType(INT64_TYPE), 3L),
                row.getValue(FOO_HEADER));

        barConverter.addLong(42L);
        assertEquals(
                new ParquetValue.Long(ParquetDataType.fromType(INT64_TYPE), 42L),
                row.getValue(BAR_HEADER));
    }

}
