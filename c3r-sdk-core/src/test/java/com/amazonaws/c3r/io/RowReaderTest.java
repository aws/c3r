// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.data.CsvRow;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.Limits;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RowReaderTest {
    @Test
    public void setTooManyRecordsInATableTest() {
        @SuppressWarnings("unchecked")
        final RowReader<CsvValue> reader = mock(RowReader.class, Mockito.CALLS_REAL_METHODS);
        when(reader.peekNextRow()).thenReturn(new CsvRow());
        assertThrows(C3rRuntimeException.class, () -> reader.setReadRowCount(Limits.ROW_COUNT_MAX + 1));
    }

    @Test
    public void readTooManyRecordsInATableTest() {
        @SuppressWarnings("unchecked")
        final RowReader<CsvValue> reader = mock(RowReader.class, Mockito.CALLS_REAL_METHODS);
        when(reader.peekNextRow()).thenReturn(new CsvRow());
        reader.setReadRowCount(Limits.ROW_COUNT_MAX);
        assertThrows(C3rRuntimeException.class, reader::next);
    }
}
