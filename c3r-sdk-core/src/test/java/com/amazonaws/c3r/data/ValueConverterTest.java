// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnInsight;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.Pad;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentMatchers;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.amazonaws.c3r.data.ClientDataType.BIGINT_BYTE_SIZE;
import static com.amazonaws.c3r.data.ClientDataType.INT_BYTE_SIZE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

public class ValueConverterTest {
    private static final ColumnInsight SEALED_COLUMN_INSIGHT = new ColumnInsight(ColumnSchema.builder().type(ColumnType.SEALED)
            .sourceHeader(ColumnHeader.ofRaw("source")).pad(Pad.DEFAULT).build());

    private static final ColumnInsight CLEARTEXT_COLUMN_INSIGHT = new ColumnInsight(ColumnSchema.builder().type(ColumnType.CLEARTEXT)
            .sourceHeader(ColumnHeader.ofRaw("source")).build());

    private static final ColumnInsight FINGERPRINT_COLUMN_INSIGHT = new ColumnInsight(ColumnSchema.builder().type(ColumnType.FINGERPRINT)
            .sourceHeader(ColumnHeader.ofRaw("source")).build());

    @ParameterizedTest
    @EnumSource(value = ClientDataType.class)
    public void cleartextMapsToSameValueTest(final ClientDataType type) {
        final byte[] testVal = new byte[]{1, 2, 3};
        final Value value = org.mockito.Mockito.mock(Value.class);
        when(value.getClientDataType()).thenReturn(type);
        when(value.getBytesAs(ArgumentMatchers.eq(ClientDataType.BIGINT)))
                .thenReturn(ByteBuffer.allocate(BIGINT_BYTE_SIZE).putLong(2L).array());
        when(value.getBytesAs(ArgumentMatchers.eq(ClientDataType.STRING))).thenReturn("hello".getBytes(StandardCharsets.UTF_8));
        when(value.getBytes()).thenReturn(testVal);
        assertEquals(value.getBytes(), ValueConverter.getBytesForColumn(value, CLEARTEXT_COLUMN_INSIGHT.getType()));
    }

    @ParameterizedTest
    @EnumSource(value = ClientDataType.class)
    public void sealedMapsToSameValueTest(final ClientDataType type) {
        final byte[] testVal = new byte[]{1, 2, 3};
        final Value value = org.mockito.Mockito.mock(Value.class);
        when(value.getClientDataType()).thenReturn(type);
        when(value.getBytesAs(ArgumentMatchers.eq(ClientDataType.BIGINT)))
                .thenReturn(ByteBuffer.allocate(BIGINT_BYTE_SIZE).putLong(2L).array());
        when(value.getBytesAs(ArgumentMatchers.eq(ClientDataType.STRING))).thenReturn("hello".getBytes(StandardCharsets.UTF_8));
        when(value.getBytes()).thenReturn(testVal);
        assertEquals(value.getBytes(), ValueConverter.getBytesForColumn(value, SEALED_COLUMN_INSIGHT.getType()));
    }

    @ParameterizedTest
    @EnumSource(value = ClientDataType.class, names = {"STRING", "CHAR", "VARCHAR"})
    public void fingerprintStringEquivalenceClassTest(final ClientDataType type) {
        final String string = "hello";
        final Value value = org.mockito.Mockito.mock(Value.class);
        when(value.getClientDataType()).thenReturn(type);
        when(value.getBytesAs(ArgumentMatchers.eq(ClientDataType.STRING))).thenReturn(string.getBytes(StandardCharsets.UTF_8));
        when(value.getBytes()).thenReturn(string.getBytes(StandardCharsets.UTF_8));
        assertArrayEquals(value.getBytes(), ValueConverter.getBytesForColumn(value, FINGERPRINT_COLUMN_INSIGHT.getType()));
    }

    @ParameterizedTest
    @EnumSource(value = ClientDataType.class, names = {"BIGINT", "INT", "SMALLINT"})
    public void fingerprintIntegerEquivalenceClassTest(final ClientDataType type) {
        final Value value = org.mockito.Mockito.mock(Value.class);
        when(value.getClientDataType()).thenReturn(type);
        when(value.getBytesAs(ArgumentMatchers.eq(ClientDataType.BIGINT)))
                .thenReturn(ByteBuffer.allocate(BIGINT_BYTE_SIZE).putLong(2L).array());
        when(value.getBytes()).thenReturn(
                type == ClientDataType.BIGINT ?
                        ByteBuffer.allocate(BIGINT_BYTE_SIZE).putLong(2L).array() : ByteBuffer.allocate(INT_BYTE_SIZE).putInt(2).array()
        );
        if (type == ClientDataType.BIGINT) {
            assertArrayEquals(value.getBytes(), ValueConverter.getBytesForColumn(value, FINGERPRINT_COLUMN_INSIGHT.getType()));
        } else {
            assertFalse(Arrays.equals(value.getBytes(), ValueConverter.getBytesForColumn(value, FINGERPRINT_COLUMN_INSIGHT.getType())));
        }
    }

    @Test
    public void bigIntBytesTest() {
        assertEquals(
                42,
                ValueConverter.BigInt.fromBytes(ValueConverter.BigInt.toBytes(42L)));
        assertNull(ValueConverter.BigInt.fromBytes(null));
        assertNull(ValueConverter.BigInt.toBytes((Integer) null));
        assertNull(ValueConverter.BigInt.toBytes((Long) null));
        // BigInt values can only be at most BIGINT_BYTE_LEN in length
        final byte[] tooFewBytes = new byte[BIGINT_BYTE_SIZE - 1];
        final byte[] exactBytes = new byte[BIGINT_BYTE_SIZE];
        final byte[] tooManyBytes = new byte[BIGINT_BYTE_SIZE + 1];
        assertDoesNotThrow(() -> ValueConverter.BigInt.fromBytes(tooFewBytes));
        assertDoesNotThrow(() -> ValueConverter.BigInt.fromBytes(exactBytes));
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.BigInt.fromBytes(tooManyBytes));
    }

    @Test
    public void booleanBytesTest() {
        assertEquals(
                true,
                ValueConverter.Boolean.fromBytes(ValueConverter.Boolean.toBytes(true)));
        assertEquals(
                false,
                ValueConverter.Boolean.fromBytes(ValueConverter.Boolean.toBytes(false)));
        assertNull(ValueConverter.Boolean.fromBytes(null));
        assertNull(ValueConverter.Boolean.fromBytes(null));
    }

    @Test
    public void dateBytesTest() {
        assertEquals(1000, ValueConverter.Date.fromBytes(ValueConverter.Date.toBytes(1000)));
        assertNull(ValueConverter.Date.fromBytes(null));
        assertNull(ValueConverter.Date.toBytes(null));
        // Date values can be only INT_BYTE_LEN long
        final byte[] tooFewBytes = new byte[INT_BYTE_SIZE - 1];
        final byte[] exactBytes = new byte[INT_BYTE_SIZE];
        final byte[] tooManyBytes = new byte[INT_BYTE_SIZE + 1];
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Date.fromBytes(tooFewBytes));
        assertDoesNotThrow(() -> ValueConverter.Date.fromBytes(exactBytes));
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Date.fromBytes(tooManyBytes));
    }

    @Test
    public void floatBytesTest() {
        assertEquals(
                (float) 42.0,
                ValueConverter.Float.fromBytes(ValueConverter.Float.toBytes((float) 42.0)));
        assertNull(ValueConverter.Float.fromBytes(null));
        assertNull(ValueConverter.Float.toBytes(null));
        // Floats must be exactly Float.BYTES long
        final byte[] tooFewBytes = new byte[Float.BYTES - 1];
        final byte[] exactBytes = new byte[Float.BYTES];
        final byte[] tooManyBytes = new byte[Float.BYTES + 1];
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Float.fromBytes(tooFewBytes));
        assertDoesNotThrow(() -> ValueConverter.Float.fromBytes(exactBytes));
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Float.fromBytes(tooManyBytes));
    }

    @Test
    public void doubleBytesTest() {
        assertEquals(
                42.0,
                ValueConverter.Double.fromBytes(ValueConverter.Double.toBytes(42.0)));
        assertNull(ValueConverter.Double.fromBytes(null));
        assertNull(ValueConverter.Double.toBytes(null));
        // Doubles must be exactly Double.BYTES long
        final byte[] tooFewBytes = new byte[Double.BYTES - 1];
        final byte[] exactBytes = new byte[Double.BYTES];
        final byte[] tooManyBytes = new byte[Double.BYTES + 1];
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Double.fromBytes(tooFewBytes));
        assertDoesNotThrow(() -> ValueConverter.Double.fromBytes(exactBytes));
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Double.fromBytes(tooManyBytes));
    }

    @Test
    public void intBytesTest() {
        assertEquals(
                42,
                ValueConverter.Int.fromBytes(ValueConverter.Int.toBytes(42)));
        assertNull(ValueConverter.Int.fromBytes(null));
        assertNull(ValueConverter.Int.toBytes(null));
        // Integer values can be at most INT_BYTES_LONG long
        final byte[] lessBytes = new byte[INT_BYTE_SIZE - 1];
        final byte[] exactBytes = new byte[INT_BYTE_SIZE];
        final byte[] tooManyBytes = new byte[INT_BYTE_SIZE + 1];
        assertDoesNotThrow(() -> ValueConverter.Int.fromBytes(lessBytes));
        assertDoesNotThrow(() -> ValueConverter.Int.fromBytes(exactBytes));
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Int.fromBytes(tooManyBytes));
    }

    @Test
    public void stringBytesTest() {
        assertEquals("hello", ValueConverter.String.fromBytes(ValueConverter.String.toBytes("hello")));
        assertNull(ValueConverter.String.fromBytes(null));
        assertNull(ValueConverter.String.toBytes(null));
    }
}
