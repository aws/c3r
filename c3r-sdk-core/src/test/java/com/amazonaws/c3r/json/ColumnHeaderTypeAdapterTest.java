// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.json;

import com.amazonaws.c3r.config.ColumnHeader;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ColumnHeaderTypeAdapterTest {

    @Test
    public void fromLowerCaseTest() {
        final ColumnHeader columnHeader = GsonUtil.fromJson("test", ColumnHeader.class);
        assertEquals("test", columnHeader.toString());
    }

    @Test
    public void fromUpperCaseTest() {
        final ColumnHeader columnHeader = GsonUtil.fromJson("TEST", ColumnHeader.class);
        assertEquals("test", columnHeader.toString());
    }

    @Test
    public void fromMixedCaseTest() {
        final ColumnHeader columnHeader = GsonUtil.fromJson("tEsT", ColumnHeader.class);
        assertEquals("test", columnHeader.toString());
    }

    @Test
    public void withLeadingSpacesTest() {
        final ColumnHeader columnHeader = GsonUtil.fromJson("  test", ColumnHeader.class);
        assertEquals("test", columnHeader.toString());
    }

    @Test
    public void withTrailingSpacesTest() {
        final ColumnHeader columnHeader = GsonUtil.fromJson("test   ", ColumnHeader.class);
        assertEquals("test", columnHeader.toString());
    }

    @Test
    public void withInnerSpacesTest() {
        // Note extra quotes in order for the JSON to grab the entire String. Not necessary in other tests.
        final ColumnHeader columnHeader = GsonUtil.fromJson("\"test test\"", ColumnHeader.class);
        assertEquals("test test", columnHeader.toString());
    }

    @Test
    public void readNullTest() {
        assertNull(GsonUtil.fromJson("null", ColumnHeader.class));
    }

    @Test
    public void writeTest() {
        final ColumnHeader columnHeader = new ColumnHeader("test");
        final String serialized = GsonUtil.toJson(columnHeader);
        final String expected = "\"" + columnHeader + "\"";
        assertEquals(expected, serialized);
    }

    @Test
    public void writeNullTest() {
        final String serialized = GsonUtil.toJson(null);
        assertEquals("null", serialized);
    }
}
