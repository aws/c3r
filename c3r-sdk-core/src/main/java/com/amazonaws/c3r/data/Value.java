// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.UnknownNullness;

/**
 * Common interface for user-provided data entries (i.e., the values that populate
 * each column).
 */
public abstract class Value {
    /**
     * Encode a non-null {@code Value} into a byte representation.
     *
     * @param value The value to encode.
     * @return The value encoded as a byte array, or {@code null} if {@code value == null}.
     */
    public static byte[] getBytes(@Nullable final Value value) {
        if (value == null) {
            return null;
        } else {
            return value.getBytes();
        }
    }

    /**
     * Whether the value is equivalent to {@code null}.
     *
     * @return {@code true} if the value represents a {@code null}
     */
    public abstract boolean isNull();

    /**
     * Get the type of then entry.
     *
     * @return Data type
     * @see ClientDataType
     */
    public abstract ClientDataType getClientDataType();

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract boolean equals(Object other);

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract int hashCode();

    /**
     * Get data type for entry.
     *
     * <p>
     * {@inheritDoc}
     */
    @Override
    @UnknownNullness
    public abstract String toString();

    /**
     * Convert the value to the specified {@code ClientDataType} if possible.
     *
     * @param type Type to format bytes as
     * @return byte representation of value converted to specified type
     */
    public abstract byte[] getBytesAs(ClientDataType type);

    /**
     * Encode a value as plaintext bytes.
     *
     * @return The underlying byte[]
     */
    public abstract byte[] getBytes();

    /**
     * Encode a value along with all metadata to accurately recreate it to a byte array.
     *
     * @return Array of bytes with all information needed to accurately recreate the value
     */
    public abstract byte[] getEncodedBytes();

    /**
     * Length of the value when byte encoded.
     *
     * @return The length of the underlying byte[]
     */
    public abstract int byteLength();
}
