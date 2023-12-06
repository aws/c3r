// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.data.ClientDataType;
import com.amazonaws.c3r.data.Value;
import com.amazonaws.c3r.data.ValueConverter;
import com.amazonaws.c3r.exception.C3rRuntimeException;
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
    @Setter
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
     * Tracks the type seen in the column so far to make sure a column doesn't contain multiple types.
     */
    @Getter
    private ClientDataType clientDataType = null;

    /**
     * Security settings in use for this collaboration.
     */
    private final ClientSettings settings;

    /**
     * Create metadata wrapper around a {@link ColumnSchema}.
     *
     * @param columnSchema Column to wrap with metadata
     * @param settings Security settings
     */
    public ColumnInsight(final ColumnSchema columnSchema, @NonNull final ClientSettings settings) {
        super(columnSchema);
        this.settings = settings;
    }

    /**
     * Updates the insight info for this column with the observed value (e.g.,
     * maximum value length thus far for this column <i>if</i>
     * {@code padType == PadType.MAX}, etc).
     *
     * @param value Seen value
     * @throws C3rRuntimeException if more than two client data types are found in a single column
     */
    public void observe(@NonNull final Value value) {
        if (value.isNull()) {
            seenNull = true;
            return;
        }
        final byte[] bytes = ValueConverter.getBytesForColumn(value, getType(), settings);
        final int length = (bytes == null) ? 0 : bytes.length;
        if (getPad() != null && getPad().getType() == PadType.MAX && maxValueLength < length) {
            maxValueLength = length;
        }

        final var clientTypeForValue = ValueConverter.getClientDataTypeForColumn(value, getType());
        if (clientDataType == null) {
            // First time observing a value, set the data type.
            clientDataType = clientTypeForValue;
        } else if (clientDataType != clientTypeForValue) {
            // This value's data type does not match the rest of the column.
            throw new C3rRuntimeException("Multiple client data types found in a single column: " + clientDataType + " and " +
                    value.getClientDataType() + ".");
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
