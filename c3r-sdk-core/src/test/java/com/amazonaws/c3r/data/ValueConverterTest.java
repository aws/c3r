// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnInsight;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.Pad;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatchers;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
                .thenReturn(ByteBuffer.allocate(ClientDataType.BIGINT_BYTE_SIZE).putLong(2L).array());
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
                .thenReturn(ByteBuffer.allocate(ClientDataType.BIGINT_BYTE_SIZE).putLong(2L).array());
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
                .thenReturn(ByteBuffer.allocate(ClientDataType.BIGINT_BYTE_SIZE).putLong(2L).array());
        when(value.getBytes()).thenReturn(
                type == ClientDataType.BIGINT ?
                        ByteBuffer.allocate(ClientDataType.BIGINT_BYTE_SIZE).putLong(2L).array() :
                        ByteBuffer.allocate(com.amazonaws.c3r.data.ClientDataType.INT_BYTE_SIZE).putInt(2).array()
        );
        if (type == ClientDataType.BIGINT) {
            assertArrayEquals(value.getBytes(), ValueConverter.getBytesForColumn(value, FINGERPRINT_COLUMN_INSIGHT.getType()));
        } else {
            assertFalse(Arrays.equals(value.getBytes(), ValueConverter.getBytesForColumn(value, FINGERPRINT_COLUMN_INSIGHT.getType())));
        }
    }

    private static Stream<Arguments> getValueConverterClasses() {
        return Stream.of(
                Arguments.of(ValueConverter.BigInt.class, List.of(Integer.class)),
                Arguments.of(ValueConverter.BigInt.class, List.of(Long.class)),
                Arguments.of(ValueConverter.Boolean.class, List.of(Boolean.class)),
                Arguments.of(ValueConverter.Char.class, List.of(String.class)),
                Arguments.of(ValueConverter.Date.class, List.of(Integer.class)),
                Arguments.of(ValueConverter.Decimal.class, List.of(BigDecimal.class)),
                Arguments.of(ValueConverter.Double.class, List.of(java.lang.Double.class)),
                Arguments.of(ValueConverter.Float.class, List.of(Float.class)),
                Arguments.of(ValueConverter.Int.class, List.of(Integer.class)),
                Arguments.of(ValueConverter.SmallInt.class, List.of(Short.class)),
                Arguments.of(ValueConverter.String.class, List.of(String.class)),
                Arguments.of(ValueConverter.Timestamp.class, List.of(Long.class, Units.Seconds.class)),
                Arguments.of(ValueConverter.Varchar.class, List.of(String.class))
        );
    }

    @ParameterizedTest
    @MethodSource("getValueConverterClasses")
    public <T> void nullToFromBytesTest(final Class clazz, final List<Class> inputs)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (inputs.size() == 1) {
            assertNull(clazz.getMethod("toBytes", inputs.get(0)).invoke(null, (Object) null));
        } else if (inputs.size() == 2) {
            assertNull(clazz.getMethod("toBytes", inputs.get(0), inputs.get(1)).invoke(null, null, null));
        } else {
            assertTrue(inputs.size() < 3);
        }
        assertNull(clazz.getMethod("fromBytes", byte[].class).invoke(null, (Object) null));
    }

    @Test
    public void bigIntBytesTest() {
        assertEquals(
                42,
                ValueConverter.BigInt.fromBytes(ValueConverter.BigInt.toBytes(42L)));
        // BigInt values can only be at most BIGINT_BYTE_LEN in length
        final byte[] tooFewBytes = new byte[ClientDataType.BIGINT_BYTE_SIZE - 1];
        final byte[] exactBytes = new byte[ClientDataType.BIGINT_BYTE_SIZE];
        final byte[] tooManyBytes = new byte[ClientDataType.BIGINT_BYTE_SIZE + 1];
        Arrays.fill(tooManyBytes, (byte) 1);
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
    }

    @Test
    public void charBytesTest() {
        final java.lang.String value = "ABC";
        assertEquals(value, ValueConverter.Char.fromBytes(ValueConverter.Char.toBytes(value)));
    }

    @Test
    public void dateBytesTest() {
        assertEquals(1000, ValueConverter.Date.fromBytes(ValueConverter.Date.toBytes(1000)));
        // Date values can be only INT_BYTE_LEN long
        final byte[] tooFewBytes = new byte[Integer.BYTES - 1];
        final byte[] exactBytes = new byte[Integer.BYTES];
        final byte[] tooManyBytes = new byte[Integer.BYTES + 1];
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Date.fromBytes(tooFewBytes));
        assertDoesNotThrow(() -> ValueConverter.Date.fromBytes(exactBytes));
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Date.fromBytes(tooManyBytes));
    }

    @Test
    public void decimalBytesTest() {
        assertEquals(BigDecimal.TEN, ValueConverter.Decimal.fromBytes(ValueConverter.Decimal.toBytes(BigDecimal.TEN)));
    }

    @Test
    public void doubleBytesTest() {
        assertEquals(
                42.0,
                ValueConverter.Double.fromBytes(ValueConverter.Double.toBytes(42.0)));
        // Doubles must be exactly Double.BYTES long
        final byte[] tooFewBytes = new byte[Double.BYTES - 1];
        final byte[] exactBytes = new byte[Double.BYTES];
        final byte[] tooManyBytes = new byte[Double.BYTES + 1];
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Double.fromBytes(tooFewBytes));
        assertDoesNotThrow(() -> ValueConverter.Double.fromBytes(exactBytes));
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Double.fromBytes(tooManyBytes));
    }

    @Test
    public void floatBytesTest() {
        assertEquals(
                (float) 42.0,
                ValueConverter.Float.fromBytes(ValueConverter.Float.toBytes((float) 42.0)));
        // Floats must be exactly Float.BYTES long
        final byte[] tooFewBytes = new byte[Float.BYTES - 1];
        final byte[] exactBytes = new byte[Float.BYTES];
        final byte[] tooManyBytes = new byte[Float.BYTES + 1];
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Float.fromBytes(tooFewBytes));
        assertDoesNotThrow(() -> ValueConverter.Float.fromBytes(exactBytes));
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Float.fromBytes(tooManyBytes));
    }

    @Test
    public void intBytesTest() {
        assertEquals(
                42,
                ValueConverter.Int.fromBytes(ValueConverter.Int.toBytes(42)));
        // Integer values can be at most INT_BYTES_LONG long
        final byte[] lessBytes = new byte[com.amazonaws.c3r.data.ClientDataType.INT_BYTE_SIZE - 1];
        final byte[] exactBytes = new byte[com.amazonaws.c3r.data.ClientDataType.INT_BYTE_SIZE];
        final byte[] tooManyBytes = new byte[com.amazonaws.c3r.data.ClientDataType.INT_BYTE_SIZE + 1];
        Arrays.fill(tooManyBytes, (byte) 9);
        assertDoesNotThrow(() -> ValueConverter.Int.fromBytes(lessBytes));
        assertDoesNotThrow(() -> ValueConverter.Int.fromBytes(exactBytes));
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Int.fromBytes(tooManyBytes));
    }

    @Test
    public void smallIntBytesTest() {
        assertEquals(
                (short) 42,
                ValueConverter.SmallInt.fromBytes(ValueConverter.SmallInt.toBytes((short) 42)));
        // SmallInt values can only be at most BIGINT_BYTE_LEN in length
        final byte[] tooFewBytes = new byte[com.amazonaws.c3r.data.ClientDataType.SMALLINT_BYTE_SIZE - 1];
        final byte[] exactBytes = new byte[com.amazonaws.c3r.data.ClientDataType.SMALLINT_BYTE_SIZE];
        final byte[] tooManyBytes = new byte[com.amazonaws.c3r.data.ClientDataType.SMALLINT_BYTE_SIZE + 1];
        Arrays.fill(tooManyBytes, (byte) 5);
        assertDoesNotThrow(() -> ValueConverter.SmallInt.fromBytes(tooFewBytes));
        assertDoesNotThrow(() -> ValueConverter.SmallInt.fromBytes(exactBytes));
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.SmallInt.fromBytes(tooManyBytes));
    }

    @Test
    public void stringBytesTest() {
        assertEquals("hello", ValueConverter.String.fromBytes(ValueConverter.String.toBytes("hello")));
    }

    @ParameterizedTest
    @EnumSource(value = Units.Seconds.class)
    public void timestampByUnitsBytesTest(final Units.Seconds unit) {
        final BigInteger expected = Units.Seconds.convert(BigInteger.valueOf(Long.MAX_VALUE), unit, Units.Seconds.NANOS);
        assertEquals(expected, ValueConverter.Timestamp.fromBytes(ValueConverter.Timestamp.toBytes(Long.MAX_VALUE, unit)));
        assertEquals(expected, ValueConverter.Timestamp.fromBytes(ValueConverter.Timestamp.toBytes(Long.MAX_VALUE, unit)));
    }

    @Test
    public void timestampBytesTest() {
        assertNull(ValueConverter.Timestamp.toBytes(null, Units.Seconds.MILLIS));
        assertNull(ValueConverter.Timestamp.toBytes(null, Units.Seconds.MILLIS));
    }

    @Test
    public void varcharBytesTest() {
        final String expected = "hello  ";
        assertEquals(expected, ValueConverter.Varchar.fromBytes(ValueConverter.Varchar.toBytes(expected)));
    }

    @ParameterizedTest
    @EnumSource(value = Units.Seconds.class)
    public void timestampByUnitsBytesTest(final Units.Seconds unit) {
        final BigInteger expected = Units.Seconds.convert(BigInteger.valueOf(Long.MAX_VALUE), unit, Units.Seconds.NANOS);
        assertEquals(expected, ValueConverter.Timestamp.fromBytes(ValueConverter.Timestamp.toBytes(Long.MAX_VALUE, unit)));
        assertEquals(expected, ValueConverter.Timestamp.fromBytes(ValueConverter.Timestamp.toBytes(Long.MAX_VALUE, unit)));
    }

    @Test
    public void timestampBytesTest() {
        assertNull(ValueConverter.Timestamp.toBytes(null, Units.Seconds.MILLIS));
        assertNull(ValueConverter.Timestamp.toBytes(null, Units.Seconds.MILLIS));
        assertNull(ValueConverter.Timestamp.toBytes(null, null));
        assertNull(ValueConverter.Timestamp.toBytes(null, null));
        assertNull(ValueConverter.Timestamp.fromBytes(null));
    }

    @Test
    public void varcharBytesTest() {
        final String expected = "hello  ";
        assertEquals(expected, ValueConverter.Varchar.fromBytes(ValueConverter.Varchar.toBytes(expected)));
        assertNull(ValueConverter.Varchar.fromBytes(null));
        assertNull(ValueConverter.Varchar.toBytes(null));
    }

    private static Stream<Arguments> encodeDecodeInputs() {
        return Stream.of(
                Arguments.of(Long.MAX_VALUE, ClientDataType.BIGINT),
                Arguments.of(true, ClientDataType.BOOLEAN),
                Arguments.of(false, ClientDataType.BOOLEAN),
                Arguments.of(Integer.MIN_VALUE, ClientDataType.DATE),
                Arguments.of(Double.MAX_VALUE, ClientDataType.DOUBLE),
                Arguments.of(Float.MIN_VALUE, ClientDataType.FLOAT),
                Arguments.of(Integer.MAX_VALUE, ClientDataType.INT),
                Arguments.of(Short.MIN_VALUE, ClientDataType.SMALLINT),
                Arguments.of("hello world", ClientDataType.STRING)
        );
    }

    private <T> Function<T, byte[]> getEncoder(final ClientDataType type) {
        switch (type) {
            case BIGINT:
                final Function<Long, byte[]> encBigInt = ValueConverter.BigInt::encode;
                return (Function<T, byte[]>) encBigInt;
            case BOOLEAN:
                final Function<Boolean, byte[]> encBool = ValueConverter.Boolean::encode;
                return (Function<T, byte[]>) encBool;
            case CHAR:
                final Function<String, byte[]> encChar = ValueConverter.Char::encode;
                return (Function<T, byte[]>) encChar;
            case DATE:
                final Function<Integer, byte[]> encDate = ValueConverter.Date::encode;
                return (Function<T, byte[]>) encDate;
            case DECIMAL:
                final Function<BigDecimal, byte[]> encDecimal = x -> ValueConverter.Decimal.encode(x, null, null);
                return (Function<T, byte[]>) encDecimal;
            case DOUBLE:
                final Function<Double, byte[]> encDouble = ValueConverter.Double::encode;
                return (Function<T, byte[]>) encDouble;
            case FLOAT:
                final Function<Float, byte[]> encFloat = ValueConverter.Float::encode;
                return (Function<T, byte[]>) encFloat;
            case INT:
                final Function<Integer, byte[]> encInt = ValueConverter.Int::encode;
                return (Function<T, byte[]>) encInt;
            case SMALLINT:
                final Function<Short, byte[]> encSmallInt = ValueConverter.SmallInt::encode;
                return (Function<T, byte[]>) encSmallInt;
            case STRING:
                final Function<String, byte[]> encString = ValueConverter.String::encode;
                return (Function<T, byte[]>) encString;
            case TIMESTAMP:
                final Function<Long, byte[]> encTimestamp = x -> ValueConverter.Timestamp.encode(x, null, null);
                return (Function<T, byte[]>) encTimestamp;
            case VARCHAR:
                final Function<String, byte[]> encVarchar = x -> ValueConverter.Varchar.encode(x, null);
                return (Function<T, byte[]>) encVarchar;
            default:
                throw new RuntimeException(type + " doesn't match an encode function.");
        }
    }

    private <T> Function<byte[], T> getDecoder(final ClientDataType type) {
        switch (type) {
            case BIGINT:
                final Function<byte[], Long> decBigInt = ValueConverter.BigInt::decode;
                return (Function<byte[], T>) decBigInt;
            case BOOLEAN:
                final Function<byte[], Boolean> decBool = ValueConverter.Boolean::decode;
                return (Function<byte[], T>) decBool;
            case CHAR:
                final Function<byte[], String> decChar = ValueConverter.Char::decode;
                return (Function<byte[], T>) decChar;
            case DATE:
                final Function<byte[], Integer> decDate = ValueConverter.Date::decode;
                return (Function<byte[], T>) decDate;
            case DECIMAL:
                final Function<byte[], ClientValueWithMetadata.Decimal> decDecimal = ValueConverter.Decimal::decode;
                return (Function<byte[], T>) decDecimal;
            case DOUBLE:
                final Function<byte[], Double> decDouble = ValueConverter.Double::decode;
                return (Function<byte[], T>) decDouble;
            case FLOAT:
                final Function<byte[], Float> decFloat = ValueConverter.Float::decode;
                return (Function<byte[], T>) decFloat;
            case INT:
                final Function<byte[], Integer> decInt = ValueConverter.Int::decode;
                return (Function<byte[], T>) decInt;
            case SMALLINT:
                final Function<byte[], Short> decSmallInt = ValueConverter.SmallInt::decode;
                return (Function<byte[], T>) decSmallInt;
            case STRING:
                final Function<byte[], String> decString = ValueConverter.String::decode;
                return (Function<byte[], T>) decString;
            case TIMESTAMP:
                final Function<byte[], ClientValueWithMetadata.Timestamp> decTimestamp = ValueConverter.Timestamp::decode;
                return (Function<byte[], T>) decTimestamp;
            case VARCHAR:
                final Function<byte[], ClientValueWithMetadata.Varchar> decVarchar = ValueConverter.Varchar::decode;
                return (Function<byte[], T>) decVarchar;
            default:
                throw new RuntimeException(type + " doesn't match an encode function.");
        }
    }

    @ParameterizedTest
    @MethodSource("encodeDecodeInputs")
    public <T> void basicEncodeDecodeTest(final T value, final ClientDataType type) {
        final Function<T, byte[]> encoder = getEncoder(type);
        final Function<byte[], T> decoder = getDecoder(type);

        assertEquals(value, decoder.apply(encoder.apply(value)));
    }

    @ParameterizedTest
    @EnumSource(value = ClientDataType.class, names = "UNKNOWN", mode = EnumSource.Mode.EXCLUDE)
    public <T> void testBadDecodeInputs(final ClientDataType type) {
        final Function<byte[], T> decoder = getDecoder(type);

        assertThrows(NullPointerException.class, () -> decoder.apply(null));

        assertThrows(C3rRuntimeException.class, () -> decoder.apply(new byte[0]));

        final byte[] badValue = ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH)
                .put(ClientDataInfo.builder().type(ClientDataType.UNKNOWN).isNull(true).build().encode())
                .array();
        assertThrows(C3rRuntimeException.class, () -> decoder.apply(badValue));

        if (type != ClientDataType.CHAR && type != ClientDataType.STRING && type != ClientDataType.VARCHAR) {
            final byte[] mismatchedNullInfo = ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH)
                    .put(ClientDataInfo.builder().type(type).isNull(false).build().encode())
                    .array();
            assertThrows(C3rRuntimeException.class, () -> decoder.apply(mismatchedNullInfo));
        }

        final byte[] extraBytes = new byte[]{ClientDataInfo.builder().type(type).isNull(true).build().encode(), (byte) 1};
        assertThrows(C3rRuntimeException.class, () -> decoder.apply(extraBytes));
    }

    @ParameterizedTest
    @EnumSource(value = ClientDataType.class, names = "UNKNOWN", mode = EnumSource.Mode.EXCLUDE)
    public <T> void nullEncodeInputTest(final ClientDataType type) {
        final Function<T, byte[]> encoder = getEncoder(type);
        final byte[] expectedBytes = new byte[]{ClientDataInfo.builder().type(type).isNull(true).build().encode()};
        assertArrayEquals(expectedBytes, encoder.apply(null));
    }

    @Test
    public void charEncodeDecodeTest() {
        final java.lang.String value = "\0\0ABC";
        assertEquals(value, ValueConverter.Char.decode(ValueConverter.Char.encode(value)));
    }

    @Test
    public void decimalEncodeDecodeTest() {
        final int scale = 3;
        final int precision = 4;
        final BigDecimal value = new BigDecimal(1.01, new MathContext(precision)).setScale(scale);

        final ClientValueWithMetadata.Decimal decodedInfo = ValueConverter.Decimal.decode(
                ValueConverter.Decimal.encode(value, precision, scale));
        assertEquals(value, decodedInfo.getValue());
        assertEquals(scale, decodedInfo.getValue().scale());
        assertEquals(scale, decodedInfo.getScale());
        assertEquals(precision, decodedInfo.getPrecision());

        final ClientValueWithMetadata.Decimal decodedNullWithMetaInfo = ValueConverter.Decimal.decode(
                ValueConverter.Decimal.encode(null, precision, scale));
        assertNotNull(decodedNullWithMetaInfo);
        assertNull(decodedNullWithMetaInfo.getValue());
        assertEquals(scale, decodedNullWithMetaInfo.getScale());
        assertEquals(precision, decodedNullWithMetaInfo.getPrecision());

        assertThrows(C3rIllegalArgumentException.class, () -> ValueConverter.Decimal.decode(
                ValueConverter.Decimal.encode(value, null, null)));
        assertThrows(C3rIllegalArgumentException.class, () -> ValueConverter.Decimal.decode(
                ValueConverter.Decimal.encode(value, precision, null)));
        assertThrows(C3rIllegalArgumentException.class, () -> ValueConverter.Decimal.decode(
                ValueConverter.Decimal.encode(value, null, scale)));

        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Decimal.encode(value, 0, scale));

        final byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
        final byte[] badPrecision = ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + 2 * Integer.BYTES + bytes.length)
                .put(ClientDataInfo.builder().type(com.amazonaws.c3r.data.ClientDataType.DECIMAL).isNull(false).build().encode())
                .putInt(0)
                .putInt(scale)
                .put(bytes)
                .array();
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Decimal.decode(badPrecision));

        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Decimal.encode(value, precision, 0));
        final byte[] badScale = ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + 2 * Integer.BYTES + bytes.length)
                .put(ClientDataInfo.builder().type(com.amazonaws.c3r.data.ClientDataType.DECIMAL).isNull(false).build().encode())
                .putInt(precision)
                .putInt(0)
                .put(bytes)
                .array();
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Decimal.decode(badScale));
    }

    @ParameterizedTest
    @EnumSource(value = Units.Seconds.class)
    public void timestampEncodeDecodeTest(final Units.Seconds unit) {
        final long value = Long.MIN_VALUE;

        final ClientValueWithMetadata.Timestamp decodedIsUtc = ValueConverter.Timestamp.decode(
                ValueConverter.Timestamp.encode(value, true, unit));
        assertEquals(value, decodedIsUtc.getValue());
        assertTrue(decodedIsUtc.getIsUtc());
        assertEquals(unit, decodedIsUtc.getUnit());

        final ClientValueWithMetadata.Timestamp decodedNotUtc = ValueConverter.Timestamp.decode(
                ValueConverter.Timestamp.encode(value, false, unit));
        assertEquals(value, decodedNotUtc.getValue());
        assertFalse(decodedNotUtc.getIsUtc());
        assertEquals(unit, decodedNotUtc.getUnit());

        assertThrows(C3rIllegalArgumentException.class, () -> ValueConverter.Timestamp.encode(value, null, unit));
        assertThrows(C3rIllegalArgumentException.class, () -> ValueConverter.Timestamp.encode(value, true, null));
        assertThrows(C3rIllegalArgumentException.class, () -> ValueConverter.Timestamp.encode(value, null, null));

        final ClientValueWithMetadata.Timestamp decodedNullWithMetadata = ValueConverter.Timestamp.decode(
                ValueConverter.Timestamp.encode(null, true, unit));
        assertNotNull(decodedNullWithMetadata);
        assertNull(decodedNullWithMetadata.getValue());
        assertEquals(true, decodedNullWithMetadata.getIsUtc());
        assertEquals(unit, decodedNullWithMetadata.getUnit());

        final byte[] badUtc = ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + Byte.BYTES + Integer.BYTES + Long.BYTES)
                .put(ClientDataInfo.builder().type(com.amazonaws.c3r.data.ClientDataType.TIMESTAMP).isNull(false).build().encode())
                .put(new byte[]{3})
                .putInt(unit.ordinal())
                .putLong(value)
                .array();
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Timestamp.decode(badUtc));

        final byte[] badUnit = ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + Byte.BYTES + Integer.BYTES + Long.BYTES)
                .put(ClientDataInfo.builder().type(com.amazonaws.c3r.data.ClientDataType.TIMESTAMP).isNull(false).build().encode())
                .put(new byte[]{1})
                .putInt(Units.Seconds.values().length + 1)
                .putLong(value)
                .array();
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Timestamp.decode(badUnit));
    }

    @Test
    public void varcharEncodeDecodeTest() {
        final String shorterValue = "hello";
        final String value = " hello ";
        final String longerValue = "hello world";
        final int maxLength = value.length();

        final ClientValueWithMetadata.Varchar shorterResult = ValueConverter.Varchar.decode(
                ValueConverter.Varchar.encode(shorterValue, maxLength));
        assertEquals(shorterValue, shorterResult.getValue());
        assertEquals(maxLength, shorterResult.getMaxLength());

        final ClientValueWithMetadata.Varchar valueResult = ValueConverter.Varchar.decode(ValueConverter.Varchar.encode(value, maxLength));
        assertEquals(value, valueResult.getValue());
        assertEquals(maxLength, valueResult.getMaxLength());

        assertThrows(C3rIllegalArgumentException.class, () -> ValueConverter.Varchar.encode(longerValue, maxLength));

        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Varchar.encode(value, null));

        final ClientValueWithMetadata.Varchar nullWithMetadataResult = ValueConverter.Varchar.decode(
                ValueConverter.Varchar.encode(null, maxLength));
        assertNotNull(nullWithMetadataResult);
        assertNull(nullWithMetadataResult.getValue());
        assertEquals(maxLength, nullWithMetadataResult.getMaxLength());

        final byte[] badLength = ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + 2 * Integer.BYTES +
                        longerValue.getBytes(StandardCharsets.UTF_8).length)
                .put(ClientDataInfo.builder().type(com.amazonaws.c3r.data.ClientDataType.VARCHAR).isNull(false).build().encode())
                .putInt(maxLength)
                .putInt(longerValue.length())
                .put(longerValue.getBytes(StandardCharsets.UTF_8))
                .array();
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Varchar.decode(badLength));
    }

    @Test
    public void nullVersusEmptyStringTypesTest() {
        final String empty = "";
        final ClientValueWithMetadata.Varchar nullMetadata = new ClientValueWithMetadata.Varchar(null, 10);
        final ClientValueWithMetadata.Varchar emptyMetadata = new ClientValueWithMetadata.Varchar(empty, 10);

        assertNull(ValueConverter.Char.decode(ValueConverter.Char.encode(null)));
        assertEquals(empty, ValueConverter.Char.decode(ValueConverter.Char.encode(empty)));

        assertNull(ValueConverter.String.decode(ValueConverter.String.encode(null)));
        assertEquals(empty, ValueConverter.String.decode(ValueConverter.String.encode(empty)));

        assertEquals(nullMetadata, ValueConverter.Varchar.decode(ValueConverter.Varchar.encode(null, emptyMetadata.getMaxLength())));
        assertEquals(emptyMetadata, ValueConverter.Varchar.decode(ValueConverter.Varchar.encode(empty, emptyMetadata.getMaxLength())));
    }

    @Test
    public void charEncodeDecodeTest() {
        final char[] value = new char[]{'\0', '\0', 'A', 'B', 'C'};

        assertArrayEquals(value, ValueConverter.Char.decode(ValueConverter.Char.encode(value)));

        assertNull(ValueConverter.Char.decode(ValueConverter.Char.encode(null)));

        assertArrayEquals(
                new byte[]{ClientDataInfo.builder().type(com.amazonaws.c3r.data.ClientDataType.CHAR).isNull(true).build().encode()},
                ValueConverter.Char.encode(null));

        assertThrows(NullPointerException.class, () -> ValueConverter.Char.decode(null));
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Char.decode(new byte[0]));

        final byte[] badValue = ByteBuffer.allocate(1)
                .put(ClientDataInfo.builder().type(ClientDataType.UNKNOWN).isNull(true).build().encode())
                .array();
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Char.decode(badValue));

        final byte[] bytes = StandardCharsets.UTF_8.encode(CharBuffer.wrap(value)).array();
        final byte[] badLength = ByteBuffer.allocate(1 + Integer.BYTES + bytes.length)
                .put(ClientDataInfo.builder().type(com.amazonaws.c3r.data.ClientDataType.CHAR).isNull(false).build().encode())
                .putInt(value.length - 1)
                .put(bytes)
                .array();
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Char.decode(badLength));
    }

    @Test
    public void decimalEncodeDecodeTest() {
        final int scale = 3;
        final int precision = 4;
        final BigDecimal value = new BigDecimal(BigInteger.TEN, scale, new MathContext(precision));

        final ClientValueWithMetadata.Decimal decodedInfo = ValueConverter.Decimal.decode(
                ValueConverter.Decimal.encode(value, precision, scale));
        assertEquals(value, decodedInfo.getValue());
        assertEquals(scale, decodedInfo.getValue().scale());
        assertEquals(scale, decodedInfo.getScale());
        assertEquals(precision, decodedInfo.getPrecision());

        final ClientValueWithMetadata.Decimal decodedNullInfo = ValueConverter.Decimal.decode(
                ValueConverter.Decimal.encode(null, null, null));
        assertNotNull(decodedNullInfo);
        assertNull(decodedNullInfo.getValue());
        assertNull(decodedNullInfo.getScale());
        assertNull(decodedNullInfo.getPrecision());

        final ClientValueWithMetadata.Decimal decodedNullWithMetaInfo = ValueConverter.Decimal.decode(
                ValueConverter.Decimal.encode(null, precision, scale));
        assertNotNull(decodedNullWithMetaInfo);
        assertNull(decodedNullWithMetaInfo.getValue());
        assertEquals(scale, decodedNullWithMetaInfo.getScale());
        assertEquals(precision, decodedNullWithMetaInfo.getPrecision());

        assertThrows(C3rIllegalArgumentException.class, () -> ValueConverter.Decimal.decode(
                ValueConverter.Decimal.encode(value, null, null)));
        assertThrows(C3rIllegalArgumentException.class, () -> ValueConverter.Decimal.decode(
                ValueConverter.Decimal.encode(value, precision, null)));
        assertThrows(C3rIllegalArgumentException.class, () -> ValueConverter.Decimal.decode(
                ValueConverter.Decimal.encode(value, null, scale)));

        final byte[] badValue = ByteBuffer.allocate(1)
                .put(ClientDataInfo.builder().type(ClientDataType.UNKNOWN).isNull(true).build().encode())
                .array();
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Decimal.decode(badValue));

        final byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
        final byte[] badPrecision = ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + 2 * Integer.BYTES + bytes.length)
                .put(ClientDataInfo.builder().type(com.amazonaws.c3r.data.ClientDataType.DECIMAL).isNull(false).build().encode())
                .putInt(precision - 1)
                .putInt(scale)
                .put(bytes)
                .array();
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Decimal.decode(badPrecision));

        final byte[] badScale = ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + 2 * Integer.BYTES + bytes.length)
                .put(ClientDataInfo.builder().type(com.amazonaws.c3r.data.ClientDataType.DECIMAL).isNull(false).build().encode())
                .putInt(precision)
                .putInt(scale + 1)
                .put(bytes)
                .array();
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Decimal.decode(badScale));
    }

    @ParameterizedTest
    @EnumSource(value = Units.Seconds.class)
    public void timestampEncodeDecodeTest(final Units.Seconds unit) {
        final long value = Long.MIN_VALUE;

        final ClientValueWithMetadata.Timestamp decodedIsUtc = ValueConverter.Timestamp.decode(
                ValueConverter.Timestamp.encode(value, true, unit));
        assertEquals(value, decodedIsUtc.getValue());
        assertTrue(decodedIsUtc.getIsUtc());
        assertEquals(unit, decodedIsUtc.getUnit());

        final ClientValueWithMetadata.Timestamp decodedNotUtc = ValueConverter.Timestamp.decode(
                ValueConverter.Timestamp.encode(value, false, unit));
        assertEquals(value, decodedNotUtc.getValue());
        assertFalse(decodedNotUtc.getIsUtc());
        assertEquals(unit, decodedNotUtc.getUnit());

        assertThrows(C3rIllegalArgumentException.class, () -> ValueConverter.Timestamp.encode(value, null, unit));
        assertThrows(C3rIllegalArgumentException.class, () -> ValueConverter.Timestamp.encode(value, true, null));
        assertThrows(C3rIllegalArgumentException.class, () -> ValueConverter.Timestamp.encode(value, null, null));

        final ClientValueWithMetadata.Timestamp decodedNull = ValueConverter.Timestamp.decode(
                ValueConverter.Timestamp.encode(null, null, null));
        assertNotNull(decodedNull);
        assertNull(decodedNull.getValue());
        assertNull(decodedNull.getIsUtc());
        assertNull(decodedNull.getUnit());

        final ClientValueWithMetadata.Timestamp decodedNullWithMetadata = ValueConverter.Timestamp.decode(
                ValueConverter.Timestamp.encode(null, true, unit));
        assertNotNull(decodedNullWithMetadata);
        assertNull(decodedNullWithMetadata.getValue());
        assertEquals(true, decodedNullWithMetadata.getIsUtc());
        assertEquals(unit, decodedNullWithMetadata.getUnit());

        final byte[] badValue = ByteBuffer.allocate(1)
                .put(ClientDataInfo.builder().type(ClientDataType.UNKNOWN).isNull(true).build().encode())
                .array();
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Timestamp.decode(badValue));

        final byte[] badUtc = ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + Byte.BYTES + Integer.BYTES + Long.BYTES)
                .put(ClientDataInfo.builder().type(com.amazonaws.c3r.data.ClientDataType.TIMESTAMP).isNull(false).build().encode())
                .put(new byte[]{3})
                .putInt(unit.ordinal())
                .putLong(value)
                .array();
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Timestamp.decode(badUtc));

        final byte[] badUnit = ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + Byte.BYTES + Integer.BYTES + Long.BYTES)
                .put(ClientDataInfo.builder().type(com.amazonaws.c3r.data.ClientDataType.TIMESTAMP).isNull(false).build().encode())
                .put(new byte[]{1})
                .putInt(Units.Seconds.values().length + 1)
                .putLong(value)
                .array();
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Timestamp.decode(badUnit));
    }

    @Test
    public void varcharEncodeDecodeTest() {
        final String shorterValue = "hello";
        final String value = " hello ";
        final String longerValue = "hello world";
        final int maxLength = value.length();

        final ClientValueWithMetadata.Varchar shorterResult = ValueConverter.Varchar.decode(
                ValueConverter.Varchar.encode(shorterValue, maxLength));
        assertEquals(shorterValue, shorterResult.getValue());
        assertEquals(maxLength, shorterResult.getMaxLength());

        final ClientValueWithMetadata.Varchar valueResult = ValueConverter.Varchar.decode(ValueConverter.Varchar.encode(value, maxLength));
        assertEquals(value, valueResult.getValue());
        assertEquals(maxLength, valueResult.getMaxLength());

        assertThrows(C3rIllegalArgumentException.class, () -> ValueConverter.Varchar.encode(longerValue, maxLength));

        final ClientValueWithMetadata.Varchar nullResult = ValueConverter.Varchar.decode(ValueConverter.Varchar.encode(null, null));
        assertNotNull(nullResult);
        assertNull(nullResult.getValue());
        assertNull(nullResult.getMaxLength());

        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Varchar.encode(value, null));

        final ClientValueWithMetadata.Varchar nullWithMetadataResult = ValueConverter.Varchar.decode(
                ValueConverter.Varchar.encode(null, maxLength));
        assertNotNull(nullWithMetadataResult);
        assertNull(nullWithMetadataResult.getValue());
        assertEquals(maxLength, nullWithMetadataResult.getMaxLength());

        final byte[] badValue = ByteBuffer.allocate(1)
                .put(ClientDataInfo.builder().type(ClientDataType.UNKNOWN).isNull(true).build().encode())
                .array();
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Varchar.decode(badValue));

        final byte[] badLength = ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + 2 * Integer.BYTES +
                        longerValue.getBytes(StandardCharsets.UTF_8).length)
                .put(ClientDataInfo.builder().type(com.amazonaws.c3r.data.ClientDataType.VARCHAR).isNull(false).build().encode())
                .putInt(maxLength)
                .putInt(longerValue.length())
                .put(longerValue.getBytes(StandardCharsets.UTF_8))
                .array();
        assertThrows(C3rRuntimeException.class, () -> ValueConverter.Varchar.decode(badLength));
    }
}
