// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.json;

import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PadTypeTypeAdapterTest {
    @Test
    public void fromLowerCaseTest() {
        final PadType padType = GsonUtil.fromJson("max", PadType.class);
        assertEquals(PadType.MAX, padType);
    }

    @Test
    public void fromUpperCaseTest() {
        final PadType padType = GsonUtil.fromJson("MAX", PadType.class);
        assertEquals(PadType.MAX, padType);
    }

    @Test
    public void fromMixedCaseTest() {
        final PadType padType = GsonUtil.fromJson("MaX", PadType.class);
        assertEquals(PadType.MAX, padType);
    }

    @Test
    public void badPadTypeTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson("bad", PadType.class));
    }

    @Test
    public void readNullTest() {
        assertNull(GsonUtil.fromJson("null", PadType.class));
    }

    @Test
    public void writeTest() {
        final String serialized = GsonUtil.toJson(PadType.MAX);
        final String expected = "\"" + PadType.MAX.name() + "\"";
        assertEquals(expected, serialized);
    }

    @Test
    public void writeNullTest() {
        final String serialized = GsonUtil.toJson(null);
        assertEquals("null", serialized);
    }
}
