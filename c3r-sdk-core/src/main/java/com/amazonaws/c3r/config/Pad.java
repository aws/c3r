// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.internal.PadUtil;
import com.amazonaws.c3r.internal.Validatable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * The pad type and pad length (if applicable) that should be used on a {@link ColumnType#SEALED} column.
 */
@EqualsAndHashCode
@Getter
public final class Pad implements Validatable {
    /**
     * Default specifications for padding.
     */
    public static final Pad DEFAULT = new Pad(PadType.NONE, null);

    /**
     * User specified padding type.
     *
     * @see PadType
     */
    private final PadType type;

    /**
     * How many bytes should be used for the pad type.
     *
     * @see PadType
     */
    private final Integer length;

    /**
     * How a {@link ColumnType#SEALED} column should be padded.
     *
     * @param type Type of padding
     * @param length Number of bytes to use with pad type
     * @see PadType
     */
    @Builder
    private Pad(final PadType type, final Integer length) {
        this.type = type;
        this.length = length;
        validate();
    }

    /**
     * Checks if the combination of pad type and length is valid.
     * <ul>
     *     <li>Padding and length must be unspecified</li>
     *     <li>If the pad type is {@link PadType#NONE} the length must be unspecified</li>
     *     <li>If the pad type is {@link PadType#FIXED} or {@link PadType#MAX}, the length is between 0 and
     *         {@link PadUtil#MAX_PAD_BYTES}</li>
     * </ul>
     *
     * @throws C3rIllegalArgumentException If the pad type and length do not follow the dependency rules
     */
    public void validate() {
        // If pad doesn't exist (type and length are null), there's nothing to validate
        if (type == null && length == null) {
            return;
        } else if (type == null) {
            throw new C3rIllegalArgumentException("A pad type is required if a pad length is specified but only a pad length was " +
                    "provided.");
        }

        // When a padLength is provided, a valid PadType must be sealed
        switch (type) {
            case NONE:
                if (length != null) {
                    throw new C3rIllegalArgumentException("A pad length was provided with an invalid pad type "
                            + type + ". A pad length is only permitted when the pad type is 'fixed' or 'max'.");
                }
                return;
            case MAX:
            case FIXED:
                if (length == null) {
                    throw new C3rIllegalArgumentException("A pad length must be provided when pad type is not 'none'.");
                } else {
                    if (length < 0 || length > PadUtil.MAX_PAD_BYTES) {
                        throw new C3rIllegalArgumentException(
                                "A pad length of " + length
                                        + " was provided provided for padded column. A pad length "
                                        + "between 0 and " + PadUtil.MAX_PAD_BYTES
                                        + " must be used when pad type is not 'none'.");
                    }
                }
                break;
            default:
                final String badType = type.toString();
                throw new C3rIllegalArgumentException("Unknown padding type " + badType + ".");
        }
    }

    /**
     * Whether two passes will be needed to process the data because of the type of padding.
     *
     * @return {@code true} if the padding is {@link PadType#FIXED} or {@link PadType#MAX}
     */
    public boolean requiresPreprocessing() {
        return type != null && type != PadType.NONE;
    }
}
