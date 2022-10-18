// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.data.Row;
import com.amazonaws.c3r.data.Value;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.Limits;
import lombok.Getter;

import java.util.List;

/**
 * Source from which records can be loaded in a streaming/as-needed fashion.
 *
 * @param <T> Specific data type being read
 */
public abstract class RowReader<T extends Value> {
    /**
     * How many rows have been read so far.
     */
    @Getter
    private long readRowCount = 0;

    /**
     * Gets the headers found in the input file, ordered as they were found.
     *
     * @return The headers of the input file
     */
    public abstract List<ColumnHeader> getHeaders();

    /**
     * Close this record source so the resource is no longer in use.
     */
    public abstract void close();

    /**
     * Called by child classes to initialize/update the {@code Row}
     * returned by {@link #peekNextRow}.
     */
    protected abstract void refreshNextRow();

    /**
     * Return the row populated from the data source by {@code refreshNextRow}.
     *
     * @return The next Row of data without advancing the reader's position
     */
    protected abstract Row<T> peekNextRow();

    /**
     * Whether there is another row remaining to be read from the source.
     *
     * @return {@code true} if there is still more data
     */
    public boolean hasNext() {
        return peekNextRow() != null;
    }

    /**
     * Returns the next row if {@code hasNext() == true}.
     *
     * @return The next row, or {@code null} if none remain
     * @throws C3rRuntimeException If the SQL row count limit is exceeded
     */
    public Row<T> next() {
        final Row<T> currentRow = peekNextRow();
        if (currentRow == null) {
            return null;
        }
        refreshNextRow();
        readRowCount++;

        if (readRowCount >= Limits.ROW_COUNT_MAX) {
            throw new C3rRuntimeException("A table cannot contain more than " + Limits.ROW_COUNT_MAX + " rows.");
        }

        return currentRow;
    }

    /**
     * Set number of already read rows in case this row reader is adding to some already existing set of rows.
     *
     * @param readRowCount How many rows have <i>already</i> been read
     * @throws C3rRuntimeException Table exceeds the maximum number of allowed rows
     */
    void setReadRowCount(final long readRowCount) {
        if (readRowCount > Limits.ROW_COUNT_MAX) {
            throw new C3rRuntimeException("A table cannot contain more than " + Limits.ROW_COUNT_MAX + " rows.");
        }
        this.readRowCount = readRowCount;
    }

    /**
     * Describes where rows are being read from in a human-friendly fashion.
     *
     * @return Name of source being used
     */
    public abstract String getSourceName();
}
