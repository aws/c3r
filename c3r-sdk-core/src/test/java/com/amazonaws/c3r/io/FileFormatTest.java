// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class FileFormatTest {
    @Test
    public void fromFileNameCsvTest() {
        assertEquals(FileFormat.CSV, FileFormat.fromFileName("hello.csv"));
    }

    @Test
    public void fromFileNameParquetTest() {
        assertEquals(FileFormat.PARQUET, FileFormat.fromFileName("hello.parquet"));
    }

    @Test
    public void fromFileNameUnknownTest() {
        assertNull(FileFormat.fromFileName("hello.unknown"));
    }

    @Test
    public void fromFileNameEmptyTest() {
        assertNull(FileFormat.fromFileName(""));
    }

    @Test
    public void fromFileNameNoSuffixTest() {
        assertNull(FileFormat.fromFileName("hello"));
    }
}
