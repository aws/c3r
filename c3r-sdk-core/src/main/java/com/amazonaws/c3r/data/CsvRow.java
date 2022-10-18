// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.internal.Nonce;
import lombok.NonNull;

/**
 * Row of CSV data.
 */
public final class CsvRow extends Row<CsvValue> {
    /**
     * Create an empty CSV row.
     */
    public CsvRow() {
    }

    /**
     * Create a fresh version of a CSV row.
     *
     * @param other Row to copy
     */
    public CsvRow(final Row<CsvValue> other) {
        other.forEach(this::putValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putBytes(@NonNull final ColumnHeader column, final byte[] encodedValue) {
        putValue(column, new CsvValue(encodedValue));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putNonce(@NonNull final ColumnHeader nonceColumn, final Nonce nonce) {
        putBytes(nonceColumn, nonce.getBytes());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Row<CsvValue> clone() {
        final Row<CsvValue> row = new CsvRow();
        getHeaders().forEach(header -> row.putValue(header, getValue(header)));
        return row;
    }
}
