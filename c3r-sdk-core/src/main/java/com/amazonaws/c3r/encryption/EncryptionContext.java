// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption;

import com.amazonaws.c3r.config.ColumnInsight;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.data.ClientDataType;
import com.amazonaws.c3r.encryption.materials.DecryptionMaterials;
import com.amazonaws.c3r.encryption.materials.EncryptionMaterials;
import com.amazonaws.c3r.encryption.providers.EncryptionMaterialsProvider;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.internal.Nonce;
import com.amazonaws.c3r.internal.Validatable;
import lombok.Builder;
import lombok.Getter;

/**
 * This class serves to provide additional useful data to {@link EncryptionMaterialsProvider}s so
 * they can more intelligently sealed the proper {@link EncryptionMaterials} or {@link
 * DecryptionMaterials} for use.
 */
@Getter
public final class EncryptionContext implements Validatable {
    /**
     * Name of the column.
     */
    private final String columnLabel;

    /**
     * Data type of field before encryption.
     */
    private final ClientDataType clientDataType;

    /**
     * Pseudorandom number.
     */
    private final Nonce nonce;

    /**
     * Type of padding used on the cleartext value, or {@code null} if padding is not applicable in this context.
     */
    private final PadType padType;

    /**
     * Length of padding to use in bytes if applicable, else {@code null}.
     */
    private final Integer padLength;

    /**
     * Maximum length in bytes of a value for this column.
     */
    private final int maxValueLength;

    /**
     * Create the configuration for encrypting this particular column.
     *
     * @param columnLabel    Name of column
     * @param nonce          Pseudorandom number
     * @param padType        Type of padding used on the column, or {@code null} if not applicable
     * @param padLength      Length of padding in bytes, or {@code null} if not applicable
     * @param maxValueLength Maximum length in bytes of the values for this context
     * @param clientDataType Data type before encryption
     */
    @Builder
    private EncryptionContext(final String columnLabel, final Nonce nonce, final PadType padType, final Integer padLength,
                              final int maxValueLength, final ClientDataType clientDataType) {
        this.columnLabel = columnLabel;
        this.nonce = nonce;
        this.padType = padType;
        this.padLength = padLength;
        this.maxValueLength = maxValueLength;
        this.clientDataType = clientDataType;

        validate();
    }

    /**
     * Create the configuration for encrypting this particular column.
     *
     * @param columnInsight  Information about the column
     * @param nonce          Pseudorandom number
     * @param clientDataType Data type before encryption
     */
    public EncryptionContext(final ColumnInsight columnInsight,
                             final Nonce nonce,
                             final ClientDataType clientDataType) {
        columnLabel = columnInsight.getTargetHeader().toString();
        this.clientDataType = clientDataType;
        this.nonce = nonce;
        padType = (columnInsight.getPad() != null) ? columnInsight.getPad().getType() : null;
        padLength = (columnInsight.getPad() != null) ? columnInsight.getPad().getLength() : null;
        maxValueLength = columnInsight.getMaxValueLength();

        validate();
    }

    /**
     * Gets the target padded length of values in the column.
     *
     * @return the target padded length for values in the column
     */
    public Integer getTargetPaddedLength() {
        if (padType == PadType.MAX) {
            return padLength + maxValueLength;
        }
        return padLength;
    }

    /**
     * Make sure column label is specified.
     */
    @Override
    public void validate() {
        if (columnLabel == null || columnLabel.isBlank()) {
            throw new C3rIllegalArgumentException("A column label must be provided in the EncryptionContext.");
        }
    }
}