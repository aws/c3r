// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.internal;

import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.data.Value;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;


/**
 * A ColumnSchema with additional state to accumulate insights about a
 * column's origin and content properties during pre-processing.
 */
@EqualsAndHashCode(callSuper = true)
public class ColumnInsight extends ColumnSchema {
    /**
     * Accumulator for maximum value length seen <i>if</i> {@code padType == PadType.MAX},
     * otherwise the field is set to {@code 0} and ignored.
     */
    @Getter
    private int maxValueLength;

    /**
     * Index of the column in the input source. A negative number indicates the column was not found.
     */
    @Getter
    @Setter
    private int sourceColumnPosition = -1;

    /**
     * Tracks whether a {@code null} value has been seen in the data processed so far.
     */
    private boolean seenNull = false;

    /**
     * Create metadata wrapper around a {@link ColumnSchema}.
     *
     * @param columnSchema Column to wrap with metadata
     */
    public ColumnInsight(final ColumnSchema columnSchema) {
        super(columnSchema);
    }

    /**
     * Updates the insight info for this column with the observed value (e.g.,
     * maximum value length thus far for this column <i>if</i>
     * {@code padType == PadType.MAX}, etc).
     *
     * @param value Seen value
     */
    public void observe(@NonNull final Value value) {
        if (value.isNull()) {
            seenNull = true;
            return;
        }
        final var length = value.byteLength();
        if (getPad() != null && getPad().getType() == PadType.MAX && maxValueLength < length) {
            maxValueLength = length;
        }
    }

    /**
     * If this column observed a value for which `isNull() == true`.
     *
     * @return False if a null value has not been encountered yet, else true
     */
    public boolean hasSeenNull() {
        return seenNull;
    }
}
