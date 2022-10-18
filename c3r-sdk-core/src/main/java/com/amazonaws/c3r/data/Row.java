// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.Nonce;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * A single row/entry in user-provided data, parameterized by the data representation
 * the of the user input.
 *
 * @param <T> Data format type
 */
@EqualsAndHashCode
@ToString
public abstract class Row<T extends Value> implements Cloneable {
    /**
     * Entries (and their corresponding header value) in a row of data.
     */
    private final Map<ColumnHeader, T> entries = new LinkedHashMap<>();

    /**
     * Get the length of the row.
     *
     * @return Length of row
     */
    public int size() {
        return entries.size();
    }

    /**
     * Get the names of the columns.
     *
     * @return Set of unique column names
     */
    public Collection<ColumnHeader> getHeaders() {
        return Collections.unmodifiableSet(entries.keySet());
    }

    /**
     * Applies a function to each header/entry pair without producing any output.
     *
     * @param action Function to apply to entry
     */
    public void forEach(final BiConsumer<? super ColumnHeader, ? super T> action) {
        entries.forEach(action);
    }

    /**
     * Adds a value to the list of row entries.
     *
     * @param column Name of the column the data belongs to
     * @param value  Row entry
     */
    public void putValue(@NonNull final ColumnHeader column, @NonNull final T value) {
        entries.put(column, value);
    }

    /**
     * Associate the given byte-encoded value with the specified column.
     *
     * @param column       Header to add nonce for
     * @param encodedValue The value (encoded as bytes) to be associated with the column
     */
    public abstract void putBytes(@NonNull ColumnHeader column, byte[] encodedValue);

    /**
     * Add a nonce value to the row. (Note: nonce values do not appear in any input/output files and may not correspond
     * to a particular format's encoding of data. They are only used internally during data marshalling.)
     *
     * @param nonceColumn The name of the column for nonce values
     * @param nonce       The nonce value to add
     */
    public abstract void putNonce(@NonNull ColumnHeader nonceColumn, Nonce nonce);

    /**
     * Checks if the entry for a column is in the row.
     *
     * @param column Column name to look for
     * @return {@code true} if row has an entry under that name
     */
    public boolean hasColumn(@NonNull final ColumnHeader column) {
        return entries.containsKey(column);
    }

    /**
     * Retrieve a value for a column in this row. If the column has no entry, and error is raised.
     *
     * @param column Name of the column to look up
     * @return Value found in column for this row
     * @throws C3rRuntimeException If there's an error looking up value for specified column in this row
     */
    public T getValue(@NonNull final ColumnHeader column) {
        final var val = entries.get(column);
        if (val == null) {
            throw new C3rRuntimeException("Row lookup error: column `" + column
                    + "` not found in known row columns ["
                    + entries.keySet().stream()
                    .map(header -> "`" + header + "`")
                    .collect(Collectors.joining(", "))
                    + "].");
        } else {
            return val;
        }
    }

    /**
     * Remove the entry for named column in the row.
     *
     * @param column Name of column to drop
     */
    public void removeColumn(final ColumnHeader column) {
        this.entries.remove(column);
    }

    /**
     * Clones the Row.
     *
     * @return A cloned copy of the row
     */
    public abstract Row<T> clone();
}
