// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rRuntimeException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CsvValueFactoryTest {
    private static final CsvValueFactory FACTORY = new CsvValueFactory();

    @Test
    public void nullValueTest() {
        assertThrows(C3rRuntimeException.class, () -> FACTORY.createValueFromEncodedBytes(null));
        assertThrows(C3rRuntimeException.class, () -> FACTORY.getValueBytesFromEncodedBytes(null));
    }

    @Test
    public void bigintTest() {
        final byte[] nullValue = ValueConverter.BigInt.encode(null);
        final CsvValue nullCsvValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullCsvValue.isNull());
        assertNull(nullCsvValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final byte[] value = ValueConverter.BigInt.encode(Long.MIN_VALUE);
        final CsvValue csvValue = FACTORY.createValueFromEncodedBytes(value);
        assertEquals(String.valueOf(Long.MIN_VALUE), csvValue.toString());

        assertArrayEquals(String.valueOf(Long.MIN_VALUE).getBytes(StandardCharsets.UTF_8), FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void booleanTest() {
        final byte[] nullValue = ValueConverter.Boolean.encode(null);
        final CsvValue nullCsvValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullCsvValue.isNull());
        assertNull(nullCsvValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final byte[] value = ValueConverter.Boolean.encode(true);
        final CsvValue csvValue = FACTORY.createValueFromEncodedBytes(value);
        assertEquals("true", csvValue.toString());

        assertArrayEquals("true".getBytes(StandardCharsets.UTF_8), FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void charTest() {
        final byte[] nullValue = ValueConverter.Char.encode(null);
        final CsvValue nullCsvValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullCsvValue.isNull());
        assertNull(nullCsvValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final byte[] value = ValueConverter.Char.encode("hello");
        final CsvValue csvValue = FACTORY.createValueFromEncodedBytes(value);
        assertEquals("hello", csvValue.toString());

        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), FACTORY.getValueBytesFromEncodedBytes(value));

    }

    @Test
    public void dateTest() {
        final byte[] nullValue = ValueConverter.Date.encode(null);
        final CsvValue nullCsvValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullCsvValue.isNull());
        assertNull(nullCsvValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final Integer date = Math.toIntExact(LocalDate.now().toEpochDay());
        final String datStr = LocalDate.ofEpochDay(date.longValue()).toString();
        final byte[] value = ValueConverter.Date.encode(date);
        final CsvValue csvValue = FACTORY.createValueFromEncodedBytes(value);
        assertEquals(datStr.toString(), csvValue.toString());

        assertArrayEquals(datStr.toString().getBytes(StandardCharsets.UTF_8), FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void decimalTest() {
        final byte[] nullValue = ValueConverter.Decimal.encode(null, null, null);
        final CsvValue nullCsvValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullCsvValue.isNull());
        assertNull(nullCsvValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final int scale = 3;
        final int precision = 4;
        final BigDecimal bigDecimal = new BigDecimal(1.01, new MathContext(precision)).setScale(scale, RoundingMode.UNNECESSARY);
        final byte[] value = ValueConverter.Decimal.encode(bigDecimal, precision, scale);
        final CsvValue csvValue = FACTORY.createValueFromEncodedBytes(value);
        assertEquals(bigDecimal.toString(), csvValue.toString());

        assertArrayEquals(bigDecimal.toString().getBytes(StandardCharsets.UTF_8), FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void doubleTest() {
        final byte[] nullValue = ValueConverter.Double.encode(null);
        final CsvValue nullCsvValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullCsvValue.isNull());
        assertNull(nullCsvValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final byte[] value = ValueConverter.Double.encode(Double.MAX_VALUE);
        final CsvValue csvValue = FACTORY.createValueFromEncodedBytes(value);
        assertEquals(String.valueOf(Double.MAX_VALUE), csvValue.toString());

        assertArrayEquals(String.valueOf(Double.MAX_VALUE).getBytes(StandardCharsets.UTF_8), FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void floatTest() {
        final byte[] nullValue = ValueConverter.Float.encode(null);
        final CsvValue nullCsvValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullCsvValue.isNull());
        assertNull(nullCsvValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final byte[] value = ValueConverter.Float.encode(Float.MIN_VALUE);
        final CsvValue csvValue = FACTORY.createValueFromEncodedBytes(value);
        assertEquals(String.valueOf(Float.MIN_VALUE), csvValue.toString());

        assertArrayEquals(String.valueOf(Float.MIN_VALUE).getBytes(StandardCharsets.UTF_8), FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void intTest() {
        final byte[] nullValue = ValueConverter.Int.encode(null);
        final CsvValue nullCsvValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullCsvValue.isNull());
        assertNull(nullCsvValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final byte[] value = ValueConverter.Int.encode(Integer.MAX_VALUE);
        final CsvValue csvValue = FACTORY.createValueFromEncodedBytes(value);
        assertEquals(String.valueOf(Integer.MAX_VALUE), csvValue.toString());

        assertArrayEquals(String.valueOf(Integer.MAX_VALUE).getBytes(StandardCharsets.UTF_8), FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void smallintTest() {
        final byte[] nullValue = ValueConverter.SmallInt.encode(null);
        final CsvValue nullCsvValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullCsvValue.isNull());
        assertNull(nullCsvValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final byte[] value = ValueConverter.SmallInt.encode(Short.MIN_VALUE);
        final CsvValue csvValue = FACTORY.createValueFromEncodedBytes(value);
        assertEquals(String.valueOf(Short.MIN_VALUE), csvValue.toString());

        assertArrayEquals(String.valueOf(Short.MIN_VALUE).getBytes(StandardCharsets.UTF_8), FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void stringTest() {
        final byte[] nullValue = ValueConverter.String.encode(null);
        final CsvValue nullCsvValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullCsvValue.isNull());
        assertNull(nullCsvValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final byte[] value = ValueConverter.String.encode("hello world");
        final CsvValue csvValue = FACTORY.createValueFromEncodedBytes(value);
        assertEquals("hello world", csvValue.toString());

        assertArrayEquals("hello world".getBytes(StandardCharsets.UTF_8), FACTORY.getValueBytesFromEncodedBytes(value));
    }

    @Test
    public void timestampTest() {
        final byte[] nullValue = ValueConverter.Timestamp.encode(null, null, null);
        final CsvValue  nullCsvValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullCsvValue.isNull());
        assertNull(nullCsvValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
        final long value = Instant.now().toEpochMilli();

        final String millisString = formatter.format(LocalDateTime.ofInstant(Instant.EPOCH.plus(value, ChronoUnit.MILLIS), ZoneOffset.UTC));
        final String millisDefaultString = formatter.format(LocalDateTime.ofInstant(Instant.EPOCH.plus(value, ChronoUnit.MILLIS),
                ZoneId.systemDefault()));
        final byte[] millisValue = ValueConverter.Timestamp.encode(value, true, Units.Seconds.MILLIS);
        assertEquals(millisString, FACTORY.createValueFromEncodedBytes(millisValue).toString());
        final byte[] millisNotUtc = ValueConverter.Timestamp.encode(value, false, Units.Seconds.MILLIS);
        assertEquals(millisDefaultString, FACTORY.createValueFromEncodedBytes(millisNotUtc).toString());
        assertArrayEquals(millisString.getBytes(StandardCharsets.UTF_8), FACTORY.getValueBytesFromEncodedBytes(millisValue));

        final String microsString = formatter.format(LocalDateTime.ofInstant(Instant.EPOCH.plus(value, ChronoUnit.MICROS), ZoneOffset.UTC));
        final String microsDefaultString = formatter.format(LocalDateTime.ofInstant(Instant.EPOCH.plus(value, ChronoUnit.MICROS),
                ZoneId.systemDefault()));
        final byte[] microsValue = ValueConverter.Timestamp.encode(value, true, Units.Seconds.MICROS);
        assertEquals(microsString, FACTORY.createValueFromEncodedBytes(microsValue).toString());
        final byte[] microsNotUtc = ValueConverter.Timestamp.encode(value, false, Units.Seconds.MICROS);
        assertEquals(microsDefaultString, FACTORY.createValueFromEncodedBytes(microsNotUtc).toString());
        assertArrayEquals(microsString.getBytes(StandardCharsets.UTF_8), FACTORY.getValueBytesFromEncodedBytes(microsValue));

        final String nanosString = formatter.format(LocalDateTime.ofInstant(Instant.EPOCH.plus(value, ChronoUnit.NANOS), ZoneOffset.UTC));
        final String nanosDefaultString = formatter.format(LocalDateTime.ofInstant(Instant.EPOCH.plus(value, ChronoUnit.NANOS),
                ZoneId.systemDefault()));
        final byte[] nanosValue = ValueConverter.Timestamp.encode(value, true, Units.Seconds.NANOS);
        assertEquals(nanosString, FACTORY.createValueFromEncodedBytes(nanosValue).toString());
        final byte[] nanosNotUtc = ValueConverter.Timestamp.encode(value, false, Units.Seconds.NANOS);
        assertEquals(nanosDefaultString, FACTORY.createValueFromEncodedBytes(nanosNotUtc).toString());
        assertArrayEquals(nanosString.getBytes(StandardCharsets.UTF_8), FACTORY.getValueBytesFromEncodedBytes(nanosValue));
    }

    @Test
    public void varcharTest() {
        final byte[] nullValue = ValueConverter.Varchar.encode(null, null);
        final CsvValue nullCsvValue = FACTORY.createValueFromEncodedBytes(nullValue);
        assertTrue(nullCsvValue.isNull());
        assertNull(nullCsvValue.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValue));

        final byte[] nullValueWithMaxLen = ValueConverter.Varchar.encode(null, 10);
        final CsvValue nullValWithMaxLen = FACTORY.createValueFromEncodedBytes(nullValueWithMaxLen);
        assertTrue(nullValWithMaxLen.isNull());
        assertNull(nullValWithMaxLen.getBytes());

        assertNull(FACTORY.getValueBytesFromEncodedBytes(nullValueWithMaxLen));

        final byte[] value = ValueConverter.Varchar.encode("hello world", 12);
        final CsvValue csvValue = FACTORY.createValueFromEncodedBytes(value);
        assertEquals("hello world", csvValue.toString());

        assertArrayEquals("hello world".getBytes(StandardCharsets.UTF_8), FACTORY.getValueBytesFromEncodedBytes(value));
    }
}
