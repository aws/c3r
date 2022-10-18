// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.CsvRow;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.data.Row;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.function.Function;

/**
 * Utilities to replace a {@link RowReader} by using mocks and value generators.
 */
public final class RowReaderTestUtility {
    /**
     * Hidden utility class constructor.
     */
    private RowReaderTestUtility() {
    }

    /**
     * Creates a mocked CSV row reader using the specified headers and value generators.
     *
     * @param headers       Names of the column headers
     * @param valueProducer Functions for generating row values
     * @param readRowCount  How many rows have been read
     * @param totalRowCount How many rows total should be produces
     * @return Mocked row reader that will return rows based on function inputs
     */
    @SuppressWarnings("unchecked")
    public static RowReader<CsvValue> getMockCsvReader(final List<ColumnHeader> headers,
                                                       final Function<ColumnHeader, String> valueProducer,
                                                       final int readRowCount,
                                                       final int totalRowCount) {
        final RowReader<CsvValue> mockReader = (RowReader<CsvValue>) Mockito.mock(RowReader.class,
                Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS));
        Mockito.when(mockReader.getHeaders()).thenReturn(headers);
        Mockito.when(mockReader.peekNextRow()).thenAnswer((Answer<?>) invocation -> {
            if (mockReader.getReadRowCount() >= totalRowCount) {
                return null;
            }
            final Row<CsvValue> row = new CsvRow();
            for (ColumnHeader header : headers) {
                row.putValue(header, new CsvValue(valueProducer.apply(header)));
            }
            return row;
        });
        mockReader.setReadRowCount(readRowCount);
        Mockito.when(mockReader.getSourceName()).thenReturn("rowSource");
        return mockReader;
    }
}
