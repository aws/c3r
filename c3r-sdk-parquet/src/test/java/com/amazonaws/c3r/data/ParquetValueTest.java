// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.COMPLEX_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.OPTIONAL_BOOLEAN_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.OPTIONAL_DOUBLE_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.OPTIONAL_FLOAT_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.OPTIONAL_INT32_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.OPTIONAL_INT64_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.OPTIONAL_STRING_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.REQUIRED_BOOLEAN_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.REQUIRED_DOUBLE_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.REQUIRED_FLOAT_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.REQUIRED_INT32_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.REQUIRED_INT64_TYPE;
import static com.amazonaws.c3r.utils.ParquetDataTypeDefsTypeTestUtility.REQUIRED_STRING_TYPE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ParquetValueTest {
    private final ParquetValue.Binary nullBinaryValue = new ParquetValue.Binary(
            ParquetDataType.fromType(OPTIONAL_STRING_TYPE),
            null);

    private final ParquetValue.Binary binaryValue = new ParquetValue.Binary(
            ParquetDataType.fromType(REQUIRED_STRING_TYPE),
            org.apache.parquet.io.api.Binary.fromReusedByteArray("foo".getBytes(StandardCharsets.UTF_8)));

    private final ParquetValue.Boolean nullBooleanValue = new ParquetValue.Boolean(
            ParquetDataType.fromType(OPTIONAL_BOOLEAN_TYPE),
            null);

    private final ParquetValue.Boolean booleanValue = new ParquetValue.Boolean(
            ParquetDataType.fromType(REQUIRED_BOOLEAN_TYPE),
            true);

    private final ParquetValue.Double nullDoubleValue = new ParquetValue.Double(
            ParquetDataType.fromType(OPTIONAL_DOUBLE_TYPE),
            null);

    private final ParquetValue.Double doubleValue = new ParquetValue.Double(
            ParquetDataType.fromType(REQUIRED_DOUBLE_TYPE),
            3.14159);

    private final ParquetValue.Float nullFloatValue = new ParquetValue.Float(
            ParquetDataType.fromType(OPTIONAL_FLOAT_TYPE),
            null);

    private final ParquetValue.Float floatValue = new ParquetValue.Float(
            ParquetDataType.fromType(REQUIRED_FLOAT_TYPE),
            3.14159f);

    private final ParquetValue.Int nullIntValue = new ParquetValue.Int(
            ParquetDataType.fromType(OPTIONAL_INT32_TYPE),
            null);

    private final ParquetValue.Int intValue = new ParquetValue.Int(
            ParquetDataType.fromType(REQUIRED_INT32_TYPE),
            3);

    private final ParquetValue.Long nullLongValue = new ParquetValue.Long(
            ParquetDataType.fromType(OPTIONAL_INT64_TYPE),
            null);

    private final ParquetValue.Long longValue = new ParquetValue.Long(
            ParquetDataType.fromType(REQUIRED_INT64_TYPE),
            3L);

    private void checkEqualsAndHashCode(final ParquetValue value1,
                                        final ParquetValue value2) {
        assertEquals(value1, value1);
        assertNotEquals(value1, value2);

        assertEquals(value1.hashCode(), value1.hashCode());
        assertNotEquals(value1.hashCode(), value2.hashCode());
    }

    @Test
    public void equalsAndHashCodeBinaryTest() {
        // same type, different values
        final var value1 = new ParquetValue.Binary(
                ParquetDataType.fromType(REQUIRED_STRING_TYPE),
                org.apache.parquet.io.api.Binary.fromReusedByteArray(new byte[]{0, 1, 2}));
        final var value2 = new ParquetValue.Binary(
                ParquetDataType.fromType(REQUIRED_STRING_TYPE),
                org.apache.parquet.io.api.Binary.fromReusedByteArray(new byte[]{3, 4, 5}));
        assertFalse(Arrays.equals(value1.getBytes(), value2.getBytes()));
        checkEqualsAndHashCode(value1, value2);

        // different type, same values
        final ParquetValue.Binary value3 = new ParquetValue.Binary(
                ParquetDataType.fromType(OPTIONAL_STRING_TYPE),
                org.apache.parquet.io.api.Binary.fromReusedByteArray(new byte[]{0, 1, 2}));
        final ParquetValue.Binary value4 = new ParquetValue.Binary(
                ParquetDataType.fromType(REQUIRED_STRING_TYPE),
                org.apache.parquet.io.api.Binary.fromReusedByteArray(new byte[]{0, 1, 2}));
        assertArrayEquals(value3.getBytes(), value4.getBytes());
        checkEqualsAndHashCode(value1, value2);
    }

    @Test
    public void equalsAndHashCodeBooleanTest() {
        // same type, different values
        final var value1 = new ParquetValue.Boolean(
                ParquetDataType.fromType(REQUIRED_BOOLEAN_TYPE),
                true);
        final var value2 = new ParquetValue.Boolean(
                ParquetDataType.fromType(REQUIRED_BOOLEAN_TYPE),
                false);
        assertFalse(Arrays.equals(value1.getBytes(), value2.getBytes()));
        checkEqualsAndHashCode(value1, value2);

        // different type, same values
        final var value3 = new ParquetValue.Boolean(
                ParquetDataType.fromType(REQUIRED_BOOLEAN_TYPE),
                true);
        final var value4 = new ParquetValue.Boolean(
                ParquetDataType.fromType(OPTIONAL_BOOLEAN_TYPE),
                true);
        assertArrayEquals(value3.getBytes(), value4.getBytes());
        checkEqualsAndHashCode(value1, value2);
    }

    @Test
    public void equalsAndHashCodeDoubleTest() {
        // same type, different values
        final var value1 = new ParquetValue.Double(
                ParquetDataType.fromType(REQUIRED_DOUBLE_TYPE),
                3.14159);
        final var value2 = new ParquetValue.Double(
                ParquetDataType.fromType(REQUIRED_DOUBLE_TYPE),
                42.0);
        assertFalse(Arrays.equals(value1.getBytes(), value2.getBytes()));
        checkEqualsAndHashCode(value1, value2);

        // different type, same values
        final var value3 = new ParquetValue.Double(
                ParquetDataType.fromType(REQUIRED_DOUBLE_TYPE),
                3.14159);
        final var value4 = new ParquetValue.Double(
                ParquetDataType.fromType(OPTIONAL_DOUBLE_TYPE),
                3.14159);
        assertArrayEquals(value3.getBytes(), value4.getBytes());
        checkEqualsAndHashCode(value1, value2);
    }

    @Test
    public void equalsAndHashCodeFloatTest() {
        // same type, different values
        final var value1 = new ParquetValue.Float(
                ParquetDataType.fromType(REQUIRED_FLOAT_TYPE),
                3.14159f);
        final var value2 = new ParquetValue.Float(
                ParquetDataType.fromType(REQUIRED_FLOAT_TYPE),
                42.0f);
        assertFalse(Arrays.equals(value1.getBytes(), value2.getBytes()));
        checkEqualsAndHashCode(value1, value2);

        // different type, same values
        final var value3 = new ParquetValue.Float(
                ParquetDataType.fromType(REQUIRED_FLOAT_TYPE),
                3.14159f);
        final var value4 = new ParquetValue.Float(
                ParquetDataType.fromType(OPTIONAL_FLOAT_TYPE),
                3.14159f);
        assertArrayEquals(value3.getBytes(), value4.getBytes());
        checkEqualsAndHashCode(value1, value2);
    }

    @Test
    public void equalsAndHashCodeIntTest() {
        // same type, different values
        final var value1 = new ParquetValue.Int(
                ParquetDataType.fromType(REQUIRED_INT32_TYPE),
                3);
        final var value2 = new ParquetValue.Int(
                ParquetDataType.fromType(REQUIRED_INT32_TYPE),
                42);
        assertFalse(Arrays.equals(value1.getBytes(), value2.getBytes()));
        checkEqualsAndHashCode(value1, value2);

        // different type, same values
        final var value3 = new ParquetValue.Int(
                ParquetDataType.fromType(REQUIRED_INT32_TYPE),
                3);
        final var value4 = new ParquetValue.Int(
                ParquetDataType.fromType(OPTIONAL_INT32_TYPE),
                3);
        assertArrayEquals(value3.getBytes(), value4.getBytes());
        checkEqualsAndHashCode(value1, value2);
    }

    @Test
    public void equalsAndHashCodeLongTest() {
        // same type, different values
        final var value1 = new ParquetValue.Long(
                ParquetDataType.fromType(REQUIRED_INT64_TYPE),
                3L);
        final var value2 = new ParquetValue.Long(
                ParquetDataType.fromType(REQUIRED_INT64_TYPE),
                42L);
        assertFalse(Arrays.equals(value1.getBytes(), value2.getBytes()));
        checkEqualsAndHashCode(value1, value2);

        // different type, same values
        final var value3 = new ParquetValue.Long(
                ParquetDataType.fromType(REQUIRED_INT64_TYPE),
                3L);
        final var value4 = new ParquetValue.Long(
                ParquetDataType.fromType(OPTIONAL_INT64_TYPE),
                3L);
        assertArrayEquals(value3.getBytes(), value4.getBytes());
        checkEqualsAndHashCode(value1, value2);
    }

    @Test
    public void getParquetDataTypeTest() {
        assertEquals(
                ParquetDataType.fromType(REQUIRED_STRING_TYPE),
                binaryValue.getParquetDataType());
        assertEquals(
                ParquetDataType.fromType(REQUIRED_BOOLEAN_TYPE),
                booleanValue.getParquetDataType());
        assertEquals(
                ParquetDataType.fromType(REQUIRED_DOUBLE_TYPE),
                doubleValue.getParquetDataType());
        assertEquals(
                ParquetDataType.fromType(REQUIRED_FLOAT_TYPE),
                floatValue.getParquetDataType());
        assertEquals(
                ParquetDataType.fromType(REQUIRED_INT32_TYPE),
                intValue.getParquetDataType());
        assertEquals(
                ParquetDataType.fromType(REQUIRED_INT64_TYPE),
                longValue.getParquetDataType());
    }

    @Test
    public void byteLengthTest() {
        assertEquals(0, nullBinaryValue.byteLength());
        assertEquals(3, binaryValue.byteLength());
        assertEquals(0, nullBooleanValue.byteLength());
        assertEquals(1, booleanValue.byteLength());
        assertEquals(0, nullDoubleValue.byteLength());
        assertEquals(Double.BYTES, doubleValue.byteLength());
        assertEquals(0, nullFloatValue.byteLength());
        assertEquals(Float.BYTES, floatValue.byteLength());
        assertEquals(0, nullIntValue.byteLength());
        assertEquals(Integer.BYTES, intValue.byteLength());
        assertEquals(0, nullLongValue.byteLength());
        assertEquals(Long.BYTES, longValue.byteLength());
    }

    @Test
    public void isNullTest() {
        assertTrue(nullBinaryValue.isNull());
        assertFalse(binaryValue.isNull());
        assertTrue(nullBooleanValue.isNull());
        assertFalse(booleanValue.isNull());
        assertTrue(nullDoubleValue.isNull());
        assertFalse(doubleValue.isNull());
        assertTrue(nullFloatValue.isNull());
        assertFalse(floatValue.isNull());
        assertTrue(nullIntValue.isNull());
        assertFalse(intValue.isNull());
        assertTrue(nullLongValue.isNull());
        assertFalse(longValue.isNull());
    }

    @Test
    public void toStringTest() {
        assertNull(nullBinaryValue.toString());
        assertEquals("foo", binaryValue.toString());
        assertNull(nullBooleanValue.toString());
        assertEquals("true", booleanValue.toString());
        assertNull(nullDoubleValue.toString());
        assertEquals("3.14159", doubleValue.toString());
        assertNull(nullFloatValue.toString());
        assertEquals("3.14159", floatValue.toString());
        assertNull(nullIntValue.toString());
        assertEquals("3", intValue.toString());
        assertNull(nullLongValue.toString());
        assertEquals("3", longValue.toString());
    }

    @Test
    public void fromBytesRoundTripTest() {
        final List<ParquetValue> values = List.of(
                binaryValue,
                booleanValue,
                doubleValue,
                floatValue,
                intValue,
                longValue
        );
        for (var value : values) {
            assertEquals(value, ParquetValue.fromBytes(value.getParquetDataType(), value.getBytes()));
        }
    }

    @Test
    public void fromBytesUnsupportedTypeTest() {
        final var unsupportedType = ParquetDataType.builder()
                .clientDataType(ClientDataType.UNKNOWN)
                .parquetType(COMPLEX_TYPE)
                .build();
        assertThrows(C3rIllegalArgumentException.class, () ->
                ParquetValue.fromBytes(unsupportedType, new byte[]{}));

    }

    @Test
    public void isExpectedTypeNonPrimitiveTest() {
        final ParquetDataType complexParquetDataType = mock(ParquetDataType.class);
        when(complexParquetDataType.getParquetType()).thenReturn(COMPLEX_TYPE);

        assertFalse(ParquetValue.isExpectedType(
                null,
                complexParquetDataType));
    }

    @Test
    public void isExpectedTypeTest() {
        final ParquetDataType int32DataType = ParquetDataType.fromType(REQUIRED_INT32_TYPE);
        final ParquetDataType int64DataType = ParquetDataType.fromType(REQUIRED_INT64_TYPE);

        assertTrue(ParquetValue.isExpectedType(
                int64DataType.getParquetType().asPrimitiveType().getPrimitiveTypeName(),
                int64DataType));
        assertFalse(ParquetValue.isExpectedType(
                int32DataType.getParquetType().asPrimitiveType().getPrimitiveTypeName(),
                int64DataType));
    }
}
