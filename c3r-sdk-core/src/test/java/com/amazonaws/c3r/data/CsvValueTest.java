// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class CsvValueTest {
    @Test
    public void stringConstructorNullTest() {
        assertNull(new CsvValue((String) null).getBytes());
        assertNull(new CsvValue((byte[]) null).getBytes());
        assertEquals(0, new CsvValue((byte[]) null).byteLength());
    }

    @Test
    public void emptyStringConstructorNullTest() {
        assertArrayEquals(new byte[0], new CsvValue("").getBytes());
        assertArrayEquals(new byte[0], new CsvValue("".getBytes(StandardCharsets.UTF_8)).getBytes());
    }

    @Test
    public void nonEmptyStringConstructorNullTest() {
        assertArrayEquals("42".getBytes(StandardCharsets.UTF_8), new CsvValue("42").getBytes());
        assertArrayEquals("42".getBytes(StandardCharsets.UTF_8), new CsvValue("42".getBytes(StandardCharsets.UTF_8)).getBytes());
    }

    @Test
    public void encodeTest() {
        assertNull(ValueConverter.String.decode(new CsvValue((String) null).getEncodedBytes()));
        assertEquals("", ValueConverter.String.decode(new CsvValue("").getEncodedBytes()));
        assertEquals("hello world!", ValueConverter.String.decode(new CsvValue("hello world!").getEncodedBytes()));
    }
}
