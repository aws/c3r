// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import lombok.Builder;
import lombok.Value;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.Objects;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * Supported Parquet types.
 */
@Value
@Builder
public class ParquetDataType {
    /**
     * The type used for nonces in Parquet.
     */
    public static final ParquetDataType NONCE_TYPE = ParquetDataType.builder()
            .clientDataType(ClientDataType.UNKNOWN)
            .parquetType(org.apache.parquet.schema.Types.required(PrimitiveTypeName.BINARY).named("nonce"))
            .build();

    /**
     * Parquet annotation type for string.
     */
    private static final LogicalTypeAnnotation STRING_LOGICAL_TYPE_ANNOTATION = LogicalTypeAnnotation.stringType();

    /**
     * Client type.
     */
    private ClientDataType clientDataType;

    /**
     * Parquet type.
     */
    private org.apache.parquet.schema.Type parquetType;

    /**
     * Checks if the Parquet type is supported by C3R.
     * Must be a primitive type that is not an {@code INT96} or {@code FIXED_LEN_BYTE_ARRAY}.
     *
     * @param type Parquet type
     * @return {@code true} if supported
     */
    public static boolean isSupportedType(final org.apache.parquet.schema.Type type) {
        return type.isPrimitive()
                && type.getRepetition() != Type.Repetition.REPEATED
                && type.asPrimitiveType().getPrimitiveTypeName() != PrimitiveTypeName.INT96
                && type.asPrimitiveType().getPrimitiveTypeName() != PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
    }

    /**
     * Checks if this is a string.
     *
     * @param type Parquet type
     * @return {@code true} if Parquet type is storing a string
     */
    static boolean isStringType(final org.apache.parquet.schema.Type type) {
        return type.isPrimitive()
                && type.asPrimitiveType().getPrimitiveTypeName().equals(PrimitiveTypeName.BINARY)
                && Objects.equals(type.getLogicalTypeAnnotation(), STRING_LOGICAL_TYPE_ANNOTATION);

    }

    /**
     * Convert a Parquet schema type to a C3R Parquet data type after verifying compatability.
     *
     * @param type Parquet type associated with a column
     * @return Instance of the associated {@code ParquetDataTy[e}
     * @throws C3rIllegalArgumentException If the data type is an unsupported Parquet data type
     */
    public static ParquetDataType fromType(final org.apache.parquet.schema.Type type) {
        if (!ParquetDataType.isSupportedType(type)) {
            throw new C3rIllegalArgumentException("Unsupported parquet type: " + type);
        }
        final boolean isString = isStringType(type);
        return ParquetDataType.builder()
                .clientDataType(isString ? ClientDataType.STRING : ClientDataType.UNKNOWN)
                .parquetType(type)
                .build();
    }

    /**
     * Creates a Parquet type with correct metadata including specified name.
     *
     * @param name Label to use for this particular type
     * @return A copy of the Parquet type with the specified name
     * @throws C3rIllegalArgumentException If the data type is an unsupported Parquet data type
     */
    public org.apache.parquet.schema.Type toTypeWithName(final String name) {
        final var primType = parquetType.asPrimitiveType().getPrimitiveTypeName();
        final var logicalAnn = parquetType.getLogicalTypeAnnotation();
        switch (parquetType.getRepetition()) {
            case OPTIONAL:
                return Types.optional(primType).as(logicalAnn).named(name);
            case REQUIRED:
                return Types.required(primType).as(logicalAnn).named(name);
            default:
                throw new C3rIllegalArgumentException("Unsupported parquet type: " + parquetType + ".");
        }
    }

    /**
     * Checks if this is a UTF8 encoded string.
     *
     * @return {@code true} if it's a UTF8 string
     */
    public boolean isStringType() {
        return ParquetDataType.isStringType(this.parquetType);
    }
}
