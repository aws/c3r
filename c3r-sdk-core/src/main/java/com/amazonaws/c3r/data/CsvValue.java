// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rRuntimeException;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.nio.charset.StandardCharsets;

/**
 * Implementation of {@link Value} for the CSV data format.
 */
@EqualsAndHashCode(callSuper = false)
public class CsvValue extends Value {
    /**
     * Data type used for CSV values.
     */
    public static final ClientDataType CLIENT_DATA_TYPE = ClientDataType.STRING;

    /**
     * Data stored as binary.
     */
    @Getter
    private final byte[] bytes;

    /**
     * Creates a CSV value containing the given String or `null`.
     *
     * @param content Data in String format (or {@code null} if the content is equivalent to SQL {@code NULL})
     */
    public CsvValue(final String content) {
        if (content == null) {
            bytes = null;
        } else {
            this.bytes = content.getBytes(StandardCharsets.UTF_8);
        }
    }

    /**
     * Creates a CSV value containing the given bytes.
     *
     * @param content Data in binary format
     */
    public CsvValue(final byte[] content) {
        if (content == null) {
            bytes = null;
        } else {
            this.bytes = content.clone();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int byteLength() {
        return (bytes == null) ? 0 : bytes.length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isNull() {
        return bytes == null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClientDataType getClientDataType() {
        return CLIENT_DATA_TYPE;
    }

    /**
     * Original string representation of the content.
     *
     * @return String representation of the value
     */
    public String toString() {
        if (bytes == null) {
            return null;
        } else {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    @Override
    public byte[] getBytesAs(final ClientDataType type) {
        if (type == ClientDataType.STRING) {
            return (bytes == null) ? null : bytes.clone();
        }
        throw new C3rRuntimeException("CsvValue could not be convered to type " + type);
    }

    @Override
    public byte[] getEncodedBytes() {
        final String value = (bytes == null) ? null : new java.lang.String(bytes, StandardCharsets.UTF_8);
        return ValueConverter.String.encode(value);
    }
}
