// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
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
    STRING((byte) 1),
    /**
     * Signed 64-bit integer.
     */
    BIGINT((byte) 2),
    /**
     * Logical boolean ({@code true}/{@code false}).
     */
    BOOLEAN((byte) 3),
    /**
     * A fixed-length UTF8-encoded string.
     */
    CHAR((byte) 4),
    /**
     * Calendar date (year, month, day).
     */
    DATE((byte) 5),
    /**
     * Exact numeric of selectable precision.
     */
    DECIMAL((byte) 6),
    /**
     * Double precision floating-point number.
     */
    DOUBLE((byte) 7),
    /**
     * Single precision floating-point number.
     */
    FLOAT((byte) 8),
    /**
     * Signed 32-bit integer.
     */
    INT((byte) 9),
    /**
     * Signed 16-bit integer.
     */
    SMALLINT((byte) 10),
    /**
     * Date and time (without time zone).
     */
    TIMESTAMP((byte) 11),
    /**
     * A variable-length character string with a user defined limit on length.
     */
    VARCHAR((byte) 12);

    /**
     * How many bits are reserved for encoding ClientDataType.
     */
    public static final int BITS = 7;

    /**
     * Max number of types representable via the fixed-width bitwise encoding of data.
     */
    public static final int MAX_DATATYPE_COUNT = (1 << ClientDataType.BITS);

    /**
     * Number of bits in a SmallInt.
     */
    public static final Integer SMALLINT_BIT_SIZE = 16;

    /**
     * Number of bytes in a SmallInt.
     */
    public static final Integer SMALLINT_BYTE_SIZE = SMALLINT_BIT_SIZE / Byte.SIZE;

    /**
     * Number of bits in an Int.
     */
    public static final Integer INT_BIT_SIZE = 32;

    /**
     * Number of bytes in an Int.
     */
    public static final Integer INT_BYTE_SIZE = INT_BIT_SIZE / Byte.SIZE;

    /**
     * Number of bits in a BigInt.
     */
    public static final Integer BIGINT_BIT_SIZE = 64;

    /**
     * Number of bytes in a BigInt.
     */
    public static final Integer BIGINT_BYTE_SIZE = BIGINT_BIT_SIZE / Byte.SIZE;

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
     * Get the representative type for an equivalence class that a type belongs to if the class supports fingerprint columns.
     * If the type isn't in a supported equivalence class, it will throw an exception.
     *
     * <ul>
     *     <li> `BOOLEAN` equivalence class:
     *     <ul>
     *         <li>Representative data type: `BOOLEAN`</li>
     *         <li>Containing data types: `BOOLEAN`</li>
     *     </ul>
     *     <li> `DATE` equivalence class:
     *     <ul>
     *         <li>Representative data type: `DATE`</li>
     *         <li>Containing data types: `DATE`</li>
     *     </ul>
     *     <li> `INTEGRAL` equivalence class:
     *     <ul>
     *         <li>Representative data type: `BIGINT`</li>
     *         <li>Containing data types: `BIGINT`, `INT`, `SMALLINT`</li>
     *     </ul>
     *     <li> `STRING` equivalence class:
     *     <ul>
     *         <li>Representative data type: `STRING`</li>
     *         <li>Containing data types: `CHAR`, `STRING`, `VARCHAR`</li>
     *     </ul>
     *     <li> `TIMESTAMP` equivalence class:
     *     <ul>
     *         <li>Representative data type: `TIMESTAMP` (in nanoseconds)</li>
     *         <li>Containing data types: `TIMESTAMP` (in milliseconds, microseconds and nanoseconds)/li>
     *     </ul>
     * </ul>
     * Types not supported by equivalence classes: {@code DECIMAL}, {@code DOUBLE}, {@code FLOAT}, {@code UNKNOWN}.
     *
     * @return The super type for the equivalence class (if one exists for this `ClientDataType`).
     * @throws C3rRuntimeException ClientDataType is unknown or is not part of a supported equivalence class
     */
    public ClientDataType getRepresentativeType() {
        switch (this) {
            case CHAR:
            case STRING:
            case VARCHAR:
                return STRING;
            case SMALLINT:
            case INT:
            case BIGINT:
                return BIGINT;
            case BOOLEAN:
                return BOOLEAN;
            case DATE:
                return DATE;
            case TIMESTAMP:
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case UNKNOWN:
                throw new C3rRuntimeException(this + " data type is not supported in Fingerprint Columns.");
            default:
                throw new C3rRuntimeException("Unknown ClientDataType: " + this);
        }
    }

    /**
     * Checks if this data type is supports fingerprint columns.
     *
     * @return {@code true} if the type can be used in a fingerprint column
     */
    public boolean supportsFingerprintColumns() {
        try {
            this.getRepresentativeType();
            return true;
        } catch (C3rRuntimeException e) {
            return false;
        }
    }

    /**
     * Check if this data type is the parent type for an equivalence class.
     *
     * @return {@code true} if this is the parent type
     */
    public boolean isEquivalenceClassRepresentativeType() {
        try {
            // A ClientDataType is an equivalence class exactly when
            // it is its own equivalence class.
            return this == this.getRepresentativeType();
        } catch (C3rRuntimeException e) {
            return false;
        }
    }
}