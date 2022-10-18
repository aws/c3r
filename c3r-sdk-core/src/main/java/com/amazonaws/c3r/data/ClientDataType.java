// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import lombok.Getter;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The various underlying data types that may be encountered during execution.
 */
public enum ClientDataType {
    /**
     * Used for uninterpreted byte values.
     */
    UNKNOWN((byte) 0),
    /**
     * UTF8-encoded string.
     */
    STRING((byte) 1);

    /**
     * How many bits are reserved for encoding ClientDataType.
     */
    public static final int BITS = 7;

    /**
     * Max number of types representable via the fixed-width bitwise encoding of data.
     */
    public static final int MAX_DATATYPE_COUNT = (1 << ClientDataType.BITS);

    /**
     * Map of the {@code ClientDataType} enum indices to the corresponding {@code ClientDataType}.
     */
    private static final Map<Byte, ClientDataType> INDEX_DATA_TYPE_MAP = Arrays.stream(ClientDataType.values())
            .collect(Collectors.toMap(ClientDataType::getIndex, Function.identity()));

    /**
     * Get the index for this particular instance.
     */
    @Getter
    private final byte index;

    /**
     * Create an enum based off of the index.
     *
     * @param index Index of the {@code ClientDataType}
     */
    ClientDataType(final byte index) {
        this.index = index;
    }

    /**
     * Look up data type by enum index.
     *
     * @param index Index of the {@link ClientDataType}
     * @return The type corresponding to {@code index}
     * @throws C3rIllegalArgumentException If an unknown data type encountered
     */
    public static ClientDataType fromIndex(final byte index) {
        final var type = INDEX_DATA_TYPE_MAP.get(index);
        if (type == null) {
            throw new C3rIllegalArgumentException("Unknown data type index: " + index);
        }
        return type;
    }

    /**
     * Whether this type supports cryptographic computing.
     *
     * @return {@code true} if this type can be used in non-cleartext columns, otherwise {@code false}
     */
    public boolean supportsCryptographicComputing() {
        return this == ClientDataType.STRING;
    }
}