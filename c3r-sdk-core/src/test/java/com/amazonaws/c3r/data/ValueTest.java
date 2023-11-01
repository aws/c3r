// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ValueTest {
    @Test
    public void getBytesReturnsNullWhenInputIsNull() {
        assertNull(Value.getBytes(null));
    }

    @Test
    public void getBytesReturnsNullWhenDataIsNull() {
        final Value value = mock(Value.class);
        when(value.getBytes()).thenReturn(null);
        assertNull(value.getBytes());
    }

    @Test public void getBytesReturnsArray() {
        final byte[] bytes = {1, 2, 3, 4};
        final Value value = mock(Value.class);
        when(value.getBytes()).thenReturn(bytes);
        // Check addresses are the same
        assertEquals(bytes, value.getBytes());
        // Check the values are the same
        assertArrayEquals(bytes, value.getBytes());
    }
}
