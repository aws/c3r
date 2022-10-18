// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Stores type and value metadata for encrypted values.
 */
@EqualsAndHashCode
@Getter
public final class ClientDataInfo {
    /**
     * How many bytes are used to store data info.
     */
    static final int BYTE_LENGTH = 1;

    /**
     * Number of metadata flags.
     */
    static final byte FLAG_COUNT = 1;

    /**
     * Flag to indicate {@code null} value.
     */
    private static final byte IS_NULL_FLAG = 0b00000001;

    /**
     * Data type being stores.
     */
    private final ClientDataType type;

    /**
     * Whether this is an encrypted {@code null} value.
     */
    private final boolean isNull;

    /**
     * Constructor for creating a byte from type information and null value status.
     *
     * @param type   {@code ClientDataType} describing this value's type information
     * @param isNull Indicates if the particular value in this row is null or not (if applicable)
     */
    @Builder
    private ClientDataInfo(final ClientDataType type, final boolean isNull) {
        this.type = type;
        this.isNull = isNull;
    }

    /**
     * Extract encoded type and value metadata from a byte value.
     *
     * @param bits Byte with type and value information
     * @return The decoded {@link ClientDataInfo}
     */
    public static ClientDataInfo decode(final byte bits) {
        final boolean isNull = (bits & IS_NULL_FLAG) != 0;
        // After extracting flags from lower bits, shift to the right
        // and get the ClientDataType in the remaining bits.
        final ClientDataType type = ClientDataType.fromIndex((byte) (bits >> FLAG_COUNT));
        return new ClientDataInfo(type, isNull);
    }

    /**
     * Combine type information and flags into a single byte for encryption.
     *
     * @return 1-byte value that contains type information for the field and an indicator if the value is {@code null}
     */
    public byte encode() {
        byte bits = (byte) (type.getIndex() << FLAG_COUNT);
        if (isNull) {
            bits |= IS_NULL_FLAG;
        }
        return bits;
    }
}
