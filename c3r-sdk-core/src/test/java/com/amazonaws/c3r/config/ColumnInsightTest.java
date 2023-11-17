// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.data.ClientDataType;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ColumnInsightTest {
    private ColumnInsight createTestSealedColumnInsight(final PadType type, final Integer length) {
        return new ColumnInsight(
                ColumnSchema.builder()
                        .pad(Pad.builder().length(length).type(type).build())
                        .sourceHeader(new ColumnHeader("source"))
                        .targetHeader(new ColumnHeader("target"))
                        .type(ColumnType.SEALED)
                        .build());
    }

    @Test
    public void maxValueLengthInitToZero() {
        final ColumnInsight columnInsight = createTestSealedColumnInsight(PadType.MAX, 42);
        assertEquals(columnInsight.getMaxValueLength(), 0);
    }

    @Test
    public void updateWhenPadTypeMax() {
        final var len9 = new CsvValue("012345678");
        final var len10 = new CsvValue("0123456789");
        final var len8 = new CsvValue("01234567");
        final ColumnInsight columnInsight = createTestSealedColumnInsight(PadType.MAX, 10);
        columnInsight.observe(len9);
        columnInsight.observe(len10);
        columnInsight.observe(len8);
        assertEquals(columnInsight.getMaxValueLength(), 10);
    }

    @Test
    public void noUpdateWhenNotPadTypeMax() {
        final ColumnInsight columnInsight = createTestSealedColumnInsight(PadType.FIXED, 42);
        columnInsight.observe(new CsvValue("0123456789"));
        assertEquals(columnInsight.getMaxValueLength(), 0);
    }

    @Test
    public void allowsUpdateWithSameType() {
        final ColumnInsight columnInsight = createTestSealedColumnInsight(PadType.NONE, null);
        columnInsight.observe(new CsvValue("test"));
        columnInsight.observe(new CsvValue((String) null));
    }

    @Test
    public void noErrorsWhenValuesAreNull() {
        final ColumnInsight columnInsight = createTestSealedColumnInsight(PadType.NONE, null);
        columnInsight.observe(new CsvValue((String) null));
        columnInsight.observe(new CsvValue((String) null));
    }

    @Test
    public void errorWhenDifferentTypesAreInColumn() {
        final ColumnInsight columnInsight = createTestSealedColumnInsight(PadType.NONE, null);
        columnInsight.observe(new CsvValue("test"));
        final CsvValue nonStringCsv = mock(CsvValue.class);
        when(nonStringCsv.getClientDataType()).thenReturn(ClientDataType.DATE);
        assertThrowsExactly(C3rRuntimeException.class, () -> columnInsight.observe(nonStringCsv));
    }
}
