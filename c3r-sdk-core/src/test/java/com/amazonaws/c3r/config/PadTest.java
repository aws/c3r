// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.reflection.ReflectionMemberAccessor;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

public class PadTest {

    @Test
    public void defaultPadTypeNoneTest() {
        final Pad pad = Pad.builder().build();
        assertNull(pad.getType());
        assertNull(pad.getLength());
    }

    @Test
    public void padLengthPadTypeMaxTest() {
        assertThrows(C3rIllegalArgumentException.class,
                () -> Pad.builder().type(PadType.MAX).build());
    }

    @Test
    public void padLengthPadTypeFixedTest() {
        assertThrows(C3rIllegalArgumentException.class,
                () -> Pad.builder().type(PadType.FIXED).build());
    }

    @Test
    public void padLengthTest() {
        final Pad pad = Pad.builder().type(PadType.MAX).length(10).build();
        assertEquals(10, pad.getLength());
    }

    @Test
    public void badPadTest() throws NoSuchFieldException, IllegalAccessException {
        final Pad p = mock(Pad.class);
        doCallRealMethod().when(p).validate();
        final var rma = new ReflectionMemberAccessor();
        final Field type = Pad.class.getDeclaredField("type");
        final Field length = Pad.class.getDeclaredField("length");
        rma.set(type, p, null);
        rma.set(length, p, 10);
        final Exception e = assertThrowsExactly(C3rIllegalArgumentException.class, p::validate);
        assertEquals("A pad type is required if a pad length is specified but only a pad length was provided.", e.getMessage());
    }
}
