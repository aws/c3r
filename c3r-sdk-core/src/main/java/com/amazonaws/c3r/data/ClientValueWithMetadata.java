// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import lombok.NonNull;
import lombok.Value;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Objects;

/**
 * Support classes for types that need additional metadata to be encoded and decoded.
 */
public final class ClientValueWithMetadata {
    /**
     * Utility class private constructor.
     */
    private ClientValueWithMetadata() {
    }

    /**
     * Checks if all metadata fields are non-null.
     *
     * @param metadataFields Array of metadata fields for the type
     * @return {@code true} if all metadata is non-null
     */
    private static boolean allMetaDataIsNonNull(@NonNull final Object[] metadataFields) {
        return Arrays.stream(metadataFields).allMatch(Objects::nonNull);
    }

    /**
     * Check if all metadata fields are null.
     *
     * @param metadataFields Array of metadata fields for the type
     * @return {@code true} if all metadata is null
     */
    private static boolean allMetaDataIsNull(@NonNull final Object[] metadataFields) {
        return Arrays.stream(metadataFields).allMatch(Objects::isNull);
    }

    /**
     * Checks that null status of the value and the metadata match. If a value is specified, all metadata must be non-null.
     * If the value is not specified, all the metadata must be null or all the metadata must be non-null.
     *
     * @param value          Value being recreated
     * @param metadataFields Array of metadata fields for the type
     * @param type           Name of the client data type for use in error messages
     * @throws C3rIllegalArgumentException if the null status of the value and metadata does not match
     */
    private static void validate(final Object value, final Object[] metadataFields, final String type) {
        if (value != null && !allMetaDataIsNonNull(metadataFields)) {
            throw new C3rIllegalArgumentException(type + " values require all metadata must be specified too.");
        } else if (value == null && !allMetaDataIsNonNull(metadataFields) && !allMetaDataIsNull(metadataFields)) {
            throw new C3rIllegalArgumentException("Metadata fields for " + type +
                    " must all be null or all be specified for null values.");
        }
    }

    /**
     * Data and metadata needed to construct a valid decimal value.
     */
    @Value
    public static class Decimal {
        /**
         * Fixed point number.
         */
        private BigDecimal value;

        /**
         * Number of digits in the entire value.
         */
        private Integer precision;

        /**
         * Number of digits to the right of the decimal in the value.
         */
        private Integer scale;

        /**
         * All information needed to recreate an instance of a decimal.
         *
         * @param value     Fixed precision number
         * @param precision How many digits are in the number
         * @param scale     How many digits are to the right of the decimal
         * @throws C3rIllegalArgumentException if value doesn't conform to precision and scale limits
         */
        public Decimal(final BigDecimal value, final Integer precision, final Integer scale) {
            final Object[] metadata = new Object[]{precision, scale};
            validate(value, metadata, "Decimal");
            this.value = value;
            this.precision = precision;
            this.scale = scale;
        }
    }

    /**
     * Data and metadata needed to construct a valid timestamp value.
     */
    @Value
    public static class Timestamp {
        /**
         * Amount of time.
         */
        private Long value;

        /**
         * If this value is adjusted to UTC.
         */
        private Boolean isUtc;

        /**
         * What unit this value is in.
         */
        private Units.Seconds unit;

        /**
         * All information needed to recreate an instance of a timestamp.
         *
         * @param value How much time
         * @param isUtc If the value is in UTC
         * @param unit  Unit of seconds
         * @throws C3rIllegalArgumentException if time value is specified but UTC or unit information is missing
         */
        public Timestamp(final Long value, final Boolean isUtc, final Units.Seconds unit) {
            final Object[] metadata = new Object[]{isUtc, unit};
            validate(value, metadata, "Timestamp");
            this.value = value;
            this.isUtc = isUtc;
            this.unit = unit;
        }
    }

    /**
     * Data and metadata needed to construct a valid variable length character array value.
     */
    @Value
    public static class Varchar {
        /**
         * Variable length character array.
         */
        private java.lang.String value;

        /**
         * Maximum length the variable character array may be.
         */
        private Integer maxLength;

        /**
         * All information needed to recreate an instance of a variable length character array.
         *
         * @param value     The variable length character value
         * @param maxLength The longest length the value can be
         * @throws C3rIllegalArgumentException if the value and maxLength are an invalid pair
         */
        public Varchar(final java.lang.String value, final Integer maxLength) {
            validate(value, new Object[]{maxLength}, "Varchar");
            if (value != null && value.length() > maxLength) {
                throw new C3rIllegalArgumentException("Value cannot be more than maxLength characters long.");
            }
            this.value = value;
            this.maxLength = maxLength;
        }
    }
}
