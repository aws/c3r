// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.internal.Validatable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.UUID;

/**
 * Column configuration data specified by a user.
 */
@EqualsAndHashCode
@Getter
public class ColumnSchema implements Validatable, Serializable {
    /**
     * What cryptographic transform to apply to data.
     */
    private final ColumnType type;

    /**
     * What types of padding should be used if column type is {@link ColumnType#SEALED}.
     *
     * @see Pad
     * @see PadType
     */
    private final Pad pad;

    /**
     * Column name/index that should be transformed into output data.
     */
    private final ColumnHeader sourceHeader;

    /**
     * Name of column to use in output data file.
     */
    private final ColumnHeader targetHeader;

    /**
     * Name of column to use internally. Generated as a UUID.
     */
    @EqualsAndHashCode.Exclude
    private final transient ColumnHeader internalHeader;

    /**
     * Creates a specification for transforming data during encryption.
     *
     * @param sourceHeader   Name or index of column in the input file
     * @param targetHeader   Name of the column in the output file
     * @param internalHeader Name of the column in the temporary SQL table
     * @param pad            What kind of padding to use if the type is {@link ColumnType#SEALED}, {@code null} otherwise
     * @param type           What cryptographic primitive should be used
     */
    @Builder
    private ColumnSchema(final ColumnHeader sourceHeader,
                         final ColumnHeader targetHeader,
                         @Nullable final ColumnHeader internalHeader,
                         final Pad pad,
                         final ColumnType type) {
        this.sourceHeader = sourceHeader;
        this.targetHeader = ColumnHeader.deriveTargetColumnHeader(sourceHeader, targetHeader, type);
        this.internalHeader = internalHeader != null ? internalHeader : new ColumnHeader(UUID.randomUUID().toString());
        this.pad = pad;
        this.type = type;
        validate();
    }

    /**
     * Copies one schema in to another.
     *
     * @param columnSchema Existing schema to copy
     */
    public ColumnSchema(final ColumnSchema columnSchema) {
        this(columnSchema.sourceHeader, columnSchema.targetHeader, columnSchema.getInternalHeader(), columnSchema.pad, columnSchema.type);
    }

    /**
     * Check that rules for a ColumnSchema are followed.
     * - A type must always be set
     * - If the column type is {@code SEALED} then there must be a pad specified
     * - Pad must not be set for other column types
     *
     * @throws C3rIllegalArgumentException If any rules are violated
     */
    public void validate() {
        // A type is always required
        if (type == null) {
            throw new C3rIllegalArgumentException("Columns must be provided a type, but column " + sourceHeader + " has none.");
        }

        // Padding must be specified on encrypted columns
        if (pad == null && type == ColumnType.SEALED) {
            throw new C3rIllegalArgumentException("Padding must be provided for sealed columns, but column " + sourceHeader + " has none.");
        }

        // Padding may only be used on encrypted columns
        if (pad != null && type != ColumnType.SEALED) {
            throw new C3rIllegalArgumentException("Padding is only available for sealed columns, but pad type " + pad.getType().name()
                    + " was sealed for column " + sourceHeader + " of type " + type + ".");
        }
    }

    /**
     * Determines if there's a need to run through the source file in order to ensure configuration constraints.
     *
     * <p>
     * A column with a {@link PadType} other than NONE would require knowing the largest data in the given column to ensure
     * padding can be done successfully and to the correct size.
     *
     * <p>
     * A column that is <b>not</b> of {@link ColumnType#CLEARTEXT} will require preprocessing in order to randomize
     * the order of output rows.
     *
     * @return {@code true} if there are any settings that require preprocessing
     */
    public boolean requiresPreprocessing() {
        boolean requiresPreprocessing = pad != null && pad.requiresPreprocessing();
        requiresPreprocessing |= type != ColumnType.CLEARTEXT;
        return requiresPreprocessing;
    }
}
