// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.json;

import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ColumnTypeTypeAdapterTest {
    @Test
    public void fromLowerCaseTest() {
        final ColumnType columnType = GsonUtil.fromJson("sealed", ColumnType.class);
        assertEquals(ColumnType.SEALED, columnType);
    }

    @Test
    public void fromUpperCaseTest() {
        final ColumnType columnType = GsonUtil.fromJson("SEALED", ColumnType.class);
        assertEquals(ColumnType.SEALED, columnType);
    }

    @Test
    public void fromMixedCaseTest() {
        final ColumnType columnType = GsonUtil.fromJson("SeaLeD", ColumnType.class);
        assertEquals(ColumnType.SEALED, columnType);
    }

    @Test
    public void badColumnTypeTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson("bad", ColumnType.class));
    }

    @Test
    public void readNullTest() {
        assertNull(GsonUtil.fromJson("null", ColumnType.class));
    }

    @Test
    public void writeTest() {
        final String serialized = GsonUtil.toJson(ColumnType.SEALED);
        final String expected = "\"" + ColumnType.SEALED + "\"";
        assertEquals(expected, serialized);
    }

    @Test
    public void writeNullTest() {
        final String serialized = GsonUtil.toJson(null);
        assertEquals("null", serialized);
    }
}
