// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ValueTest {

    @Test
    public void booleanBytesTest() {
        assertEquals(
                true,
                Value.booleanFromBytes(Value.booleanToBytes(true)));
        assertEquals(
                false,
                Value.booleanFromBytes(Value.booleanToBytes(false)));
        assertNull(Value.booleanFromBytes(null));
    }

    @Test
    public void intBytesTest() {
        assertEquals(
                42,
                Value.intFromBytes(Value.intToBytes(42)));
        assertNull(Value.intFromBytes(null));
        // Integer values can be at most Integer.BYTES long
        final byte[] shortBytes = new byte[Integer.BYTES - 1];
        final byte[] exactBytes = new byte[Integer.BYTES];
        final byte[] longBytes = new byte[Integer.BYTES + 1];
        assertDoesNotThrow(() -> Value.intFromBytes(shortBytes));
        assertDoesNotThrow(() -> Value.intFromBytes(exactBytes));
        assertThrows(C3rRuntimeException.class, () -> Value.intFromBytes(longBytes));
    }

    @Test
    public void longBytesTest() {
        assertEquals(
                42,
                Value.longFromBytes(Value.longToBytes(42L)));
        assertNull(Value.longFromBytes(null));
        // Long values can only be at most Long.BYTES in length
        final byte[] shortBytes = new byte[Long.BYTES - 1];
        final byte[] exactBytes = new byte[Long.BYTES];
        final byte[] longBytes = new byte[Long.BYTES + 1];
        assertDoesNotThrow(() -> Value.longFromBytes(shortBytes));
        assertDoesNotThrow(() -> Value.longFromBytes(exactBytes));
        assertThrows(C3rRuntimeException.class, () -> Value.longFromBytes(longBytes));
    }

    @Test
    public void floatBytesTest() {
        assertEquals(
                (float) 42.0,
                Value.floatFromBytes(Value.floatToBytes((float) 42.0)));
        assertNull(Value.floatFromBytes(null));
        // Floats must be exactly Float.BYTES long
        final byte[] shortBytes = new byte[Float.BYTES - 1];
        final byte[] exactBytes = new byte[Float.BYTES];
        final byte[] longBytes = new byte[Float.BYTES + 1];
        assertThrows(C3rRuntimeException.class, () -> Value.floatFromBytes(shortBytes));
        assertDoesNotThrow(() -> Value.floatFromBytes(exactBytes));
        assertThrows(C3rRuntimeException.class, () -> Value.floatFromBytes(longBytes));
    }

    @Test
    public void doubleBytesTest() {
        assertEquals(
                42.0,
                Value.doubleFromBytes(Value.doubleToBytes(42.0)));
        assertNull(Value.doubleFromBytes(null));
        // Doubles must be exactly Double.BYTES long
        final byte[] shortBytes = new byte[Double.BYTES - 1];
        final byte[] exactBytes = new byte[Double.BYTES];
        final byte[] longBytes = new byte[Double.BYTES + 1];
        assertThrows(C3rRuntimeException.class, () -> Value.doubleFromBytes(shortBytes));
        assertDoesNotThrow(() -> Value.doubleFromBytes(exactBytes));
        assertThrows(C3rRuntimeException.class, () -> Value.doubleFromBytes(longBytes));
    }
}
