// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rRuntimeException;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ParquetValueFactoryTest {
    private static final ParquetValueFactory FACTORY = new ParquetValueFactory(null);

    @Test
    public void nullValueTest() {
        assertThrows(C3rRuntimeException.class, () -> FACTORY.createValueFromEncodedBytes(null));
        assertThrows(C3rRuntimeException.class, () -> FACTORY.getValueBytesFromEncodedBytes(null));
    }

    @Test
    public void bigintTest() {
        final byte[] nullValue = ValueConverter.BigInt.encode(null);
        final ParquetValue nullParquetValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullParquetValue.isNull());
        assertNull(nullParquetValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final byte[] value = ValueConverter.BigInt.encode(Long.MIN_VALUE);
        final ParquetValue parquetValue = FACTORY.createValueFromEncodedBytes(value);
        assertArrayEquals(ByteBuffer.allocate(Long.BYTES).putLong(Long.MIN_VALUE).array(), parquetValue.getBytes());
        assertEquals(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named(""), parquetValue.getParquetDataType().getParquetType());
        assertEquals(ClientDataType.BIGINT, parquetValue.getClientDataType());

        assertArrayEquals(ByteBuffer.allocate(Long.BYTES).putLong(Long.MIN_VALUE).array(), FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void booleanTest() {
        final byte[] nullValue = ValueConverter.Boolean.encode(null);
        final ParquetValue nullParquetValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullParquetValue.isNull());
        assertNull(nullParquetValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final byte[] value = ValueConverter.Boolean.encode(true);
        final ParquetValue parquetValue = FACTORY.createValueFromEncodedBytes(value);
        assertArrayEquals(ByteBuffer.allocate(1).put((byte) 1).array(), parquetValue.getBytes());
        assertEquals(Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(""), parquetValue.getParquetDataType().getParquetType());
        assertEquals(ClientDataType.BOOLEAN, parquetValue.getClientDataType());

        assertArrayEquals(ByteBuffer.allocate(1).put((byte) 1).array(), FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void charTest() {
        final byte[] nullValue = ValueConverter.Char.encode(null);
        final ParquetValue nullParquetValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullParquetValue.isNull());
        assertNull(nullParquetValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final byte[] value = ValueConverter.Char.encode("hello");
        final byte[] nonEncodedValue = "hello".getBytes(StandardCharsets.UTF_8);
        final ParquetValue parquetValue = FACTORY.createValueFromEncodedBytes(value);
        assertArrayEquals(nonEncodedValue, parquetValue.getBytes());
        assertEquals(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(""),
                parquetValue.getParquetDataType().getParquetType());
        assertEquals(ClientDataType.STRING, parquetValue.getClientDataType());

        assertArrayEquals(nonEncodedValue, FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void dateTest() {
        final byte[] nullValue = ValueConverter.Date.encode(null);
        final ParquetValue nullParquetValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullParquetValue.isNull());
        assertNull(nullParquetValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final Integer date = Math.toIntExact(LocalDate.now().toEpochDay());
        final byte[] value = ValueConverter.Date.encode(date);
        final ParquetValue parquetValue = FACTORY.createValueFromEncodedBytes(value);
        assertArrayEquals(ByteBuffer.allocate(Integer.BYTES).putInt(date).array(), parquetValue.getBytes());
        assertEquals(Types.optional(PrimitiveType.PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named(""),
                parquetValue.getParquetDataType().getParquetType());
        assertEquals(ClientDataType.DATE, parquetValue.getClientDataType());

        assertArrayEquals(ByteBuffer.allocate(Integer.BYTES).putInt(date).array(), FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void decimalTest() {
        final byte[] nullValue = ValueConverter.Decimal.encode(null, null, null);
        final ParquetValue nullParquetValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullParquetValue.isNull());
        assertNull(nullParquetValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final int scale = 3;
        final int precision = 4;
        final BigDecimal bigDecimal = new BigDecimal(1.01, new MathContext(precision)).setScale(scale, RoundingMode.UNNECESSARY);
        final byte[] nonEncodedValue = bigDecimal.unscaledValue().toByteArray();
        final byte[] value = ValueConverter.Decimal.encode(bigDecimal, precision, scale);
        final ParquetValue parquetValue = FACTORY.createValueFromEncodedBytes(value);
        assertArrayEquals(nonEncodedValue, parquetValue.getBytes());
        assertEquals(
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.decimalType(scale, precision)).named(""),
                parquetValue.getParquetDataType().getParquetType());
        assertEquals(ClientDataType.DECIMAL, parquetValue.getClientDataType());

        assertArrayEquals(nonEncodedValue, FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void doubleTest() {
        final byte[] nullValue = ValueConverter.Double.encode(null);
        final ParquetValue nullParquetValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullParquetValue.isNull());
        assertNull(nullParquetValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final byte[] value = ValueConverter.Double.encode(Double.MAX_VALUE);
        final ParquetValue parquetValue = FACTORY.createValueFromEncodedBytes(value);
        assertArrayEquals(ByteBuffer.allocate(Double.BYTES).putDouble(Double.MAX_VALUE).array(), parquetValue.getBytes());
        assertEquals(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(""), parquetValue.getParquetDataType().getParquetType());
        assertEquals(ClientDataType.DOUBLE, parquetValue.getClientDataType());

        assertArrayEquals(ByteBuffer.allocate(Double.BYTES).putDouble(Double.MAX_VALUE).array(),
                FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void floatTest() {
        final byte[] nullValue = ValueConverter.Float.encode(null);
        final ParquetValue nullParquetValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullParquetValue.isNull());
        assertNull(nullParquetValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final byte[] value = ValueConverter.Float.encode(Float.MIN_VALUE);
        final ParquetValue parquetValue = FACTORY.createValueFromEncodedBytes(value);
        assertArrayEquals(ByteBuffer.allocate(Float.BYTES).putFloat(Float.MIN_VALUE).array(), parquetValue.getBytes());
        assertEquals(Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named(""), parquetValue.getParquetDataType().getParquetType());
        assertEquals(ClientDataType.FLOAT, parquetValue.getClientDataType());

        assertArrayEquals(ByteBuffer.allocate(Float.BYTES).putFloat(Float.MIN_VALUE).array(), FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void intTest() {
        final byte[] nullValue = ValueConverter.Int.encode(null);
        final ParquetValue nullParquetValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullParquetValue.isNull());
        assertNull(nullParquetValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final byte[] value = ValueConverter.Int.encode(Integer.MAX_VALUE);
        final ParquetValue parquetValue = FACTORY.createValueFromEncodedBytes(value);
        assertArrayEquals(ByteBuffer.allocate(Integer.BYTES).putInt(Integer.MAX_VALUE).array(), parquetValue.getBytes());
        assertEquals(Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named(""), parquetValue.getParquetDataType().getParquetType());
        assertEquals(ClientDataType.INT, parquetValue.getClientDataType());

        assertArrayEquals(ByteBuffer.allocate(Integer.BYTES).putInt(Integer.MAX_VALUE).array(),
                FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void smallintTest() {
        final byte[] nullValue = ValueConverter.SmallInt.encode(null);
        final ParquetValue nullParquetValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullParquetValue.isNull());
        assertNull(nullParquetValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final byte[] value = ValueConverter.SmallInt.encode(Short.MIN_VALUE);
        final ParquetValue parquetValue = FACTORY.createValueFromEncodedBytes(value);
        assertArrayEquals(ByteBuffer.allocate(Integer.BYTES).putInt(Short.MIN_VALUE).array(), parquetValue.getBytes());
        assertEquals(Types.optional(PrimitiveType.PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.intType(16, true)).named(""),
                parquetValue.getParquetDataType().getParquetType());
        assertEquals(ClientDataType.SMALLINT, parquetValue.getClientDataType());

        assertArrayEquals(ByteBuffer.allocate(Integer.BYTES).putInt(Short.MIN_VALUE).array(), FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void stringTest() {
        final byte[] nullValue = ValueConverter.String.encode(null);
        final ParquetValue nullParquetValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullParquetValue.isNull());
        assertNull(nullParquetValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final byte[] value = ValueConverter.String.encode("hello world");
        final byte[] nonEncodedValue = "hello world".getBytes(StandardCharsets.UTF_8);
        final ParquetValue parquetValue = FACTORY.createValueFromEncodedBytes(value);
        assertArrayEquals(nonEncodedValue, parquetValue.getBytes());
        assertEquals(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(""),
                parquetValue.getParquetDataType().getParquetType());
        assertEquals(ClientDataType.STRING, parquetValue.getClientDataType());

        assertArrayEquals(nonEncodedValue, FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void timestampTest() {
        final byte[] nullValue = ValueConverter.Timestamp.encode(null, null, null);
        final ParquetValue  nullParquetValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullParquetValue.isNull());
        assertNull(nullParquetValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final long value = Instant.now().toEpochMilli();

        final byte[] millisValue = ValueConverter.Timestamp.encode(value, true, Units.Seconds.MILLIS);
        final ParquetValue parquetValueMillis = FACTORY.createValueFromEncodedBytes(millisValue);
        assertArrayEquals(ByteBuffer.allocate(Long.BYTES).putLong(value).array(), parquetValueMillis.getBytes());
        assertEquals(
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                        .named(""),
                parquetValueMillis.getParquetDataType().getParquetType());
        assertEquals(ClientDataType.TIMESTAMP, parquetValueMillis.getClientDataType());
        assertArrayEquals(ByteBuffer.allocate(Long.BYTES).putLong(value).array(), FACTORY.getValueBytesFromEncodedBytes(millisValue));

        final byte[] millisNotUtcValue = ValueConverter.Timestamp.encode(value, false, Units.Seconds.MILLIS);
        final ParquetValue parquetValueMillisNotUtc = FACTORY.createValueFromEncodedBytes(millisNotUtcValue);
        assertArrayEquals(ByteBuffer.allocate(Long.BYTES).putLong(value).array(), parquetValueMillisNotUtc.getBytes());
        assertEquals(
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                        .named(""),
                parquetValueMillisNotUtc.getParquetDataType().getParquetType());
        assertEquals(ClientDataType.TIMESTAMP, parquetValueMillisNotUtc.getClientDataType());
        assertArrayEquals(ByteBuffer.allocate(Long.BYTES).putLong(value).array(), FACTORY.getValueBytesFromEncodedBytes(millisNotUtcValue));

        final byte[] microsValue = ValueConverter.Timestamp.encode(value, true, Units.Seconds.MICROS);
        final ParquetValue parquetValueMicros = FACTORY.createValueFromEncodedBytes(microsValue);
        assertArrayEquals(ByteBuffer.allocate(Long.BYTES).putLong(value).array(), parquetValueMicros.getBytes());
        assertEquals(
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                        .named(""),
                parquetValueMicros.getParquetDataType().getParquetType());
        assertEquals(ClientDataType.TIMESTAMP, parquetValueMicros.getClientDataType());
        assertArrayEquals(ByteBuffer.allocate(Long.BYTES).putLong(value).array(), FACTORY.getValueBytesFromEncodedBytes(microsValue));

        final byte[] microsNotUtcValue = ValueConverter.Timestamp.encode(value, false, Units.Seconds.MICROS);
        final ParquetValue parquetValueMicrosNotUtc = FACTORY.createValueFromEncodedBytes(microsNotUtcValue);
        assertArrayEquals(ByteBuffer.allocate(Long.BYTES).putLong(value).array(), parquetValueMicrosNotUtc.getBytes());
        assertEquals(
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                        .named(""),
                parquetValueMicrosNotUtc.getParquetDataType().getParquetType());
        assertEquals(ClientDataType.TIMESTAMP, parquetValueMicrosNotUtc.getClientDataType());
        assertArrayEquals(ByteBuffer.allocate(Long.BYTES).putLong(value).array(), FACTORY.getValueBytesFromEncodedBytes(microsNotUtcValue));

        final byte[] nanosValue = ValueConverter.Timestamp.encode(value, true, Units.Seconds.NANOS);
        final ParquetValue parquetValueNanos = FACTORY.createValueFromEncodedBytes(nanosValue);
        assertArrayEquals(ByteBuffer.allocate(Long.BYTES).putLong(value).array(), parquetValueNanos.getBytes());
        assertEquals(
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
                        .named(""),
                parquetValueNanos.getParquetDataType().getParquetType());
        assertEquals(ClientDataType.TIMESTAMP, parquetValueNanos.getClientDataType());
        assertArrayEquals(ByteBuffer.allocate(Long.BYTES).putLong(value).array(), FACTORY.getValueBytesFromEncodedBytes(nanosValue));

        final byte[] nanosNotUtcValue = ValueConverter.Timestamp.encode(value, false, Units.Seconds.NANOS);
        final ParquetValue parquetValueNanosNotUtc = FACTORY.createValueFromEncodedBytes(nanosNotUtcValue);
        assertArrayEquals(ByteBuffer.allocate(Long.BYTES).putLong(value).array(), parquetValueNanosNotUtc.getBytes());
        assertEquals(
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS))
                        .named(""),
                parquetValueNanosNotUtc.getParquetDataType().getParquetType());
        assertEquals(ClientDataType.TIMESTAMP, parquetValueNanosNotUtc.getClientDataType());
        assertArrayEquals(ByteBuffer.allocate(Long.BYTES).putLong(value).array(), FACTORY.getValueBytesFromEncodedBytes(nanosNotUtcValue));
    }

    @Test
    public void varcharTest() {
        final byte[] nullValue = ValueConverter.Varchar.encode(null, null);
        final ParquetValue nullParquetValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullParquetValue.isNull());
        assertNull(nullParquetValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final byte[] nullValueWithMaxLen = ValueConverter.Varchar.encode(null, 10);
        final ParquetValue nullValWithMaxLen = FACTORY.createValueFromEncodedBytes(nullValueWithMaxLen);
        assertTrue(nullValWithMaxLen.isNull());
        assertNull(nullValWithMaxLen.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValueWithMaxLen));

        final byte[] value = ValueConverter.Varchar.encode("hello world", 12);
        final byte[] nonEncodedValue = "hello world".getBytes(StandardCharsets.UTF_8);
        final ParquetValue parquetValue = FACTORY.createValueFromEncodedBytes(value);
        assertArrayEquals(nonEncodedValue, parquetValue.getBytes());
        assertEquals(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(""),
                parquetValue.getParquetDataType().getParquetType());
        assertEquals(ClientDataType.STRING, parquetValue.getClientDataType());

        assertArrayEquals(nonEncodedValue, FACTORY.getValueBytesFromEncodedBytes(value));
    }
}
