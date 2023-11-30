// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import lombok.NonNull;

import java.math.BigInteger;

/**
 * Units used in C3R data types.
 */
public class Units {
    /**
     * Supported second based time units and a helper function to convert between units.
     */
    public enum Seconds {
        /**
         * Milliseconds.
         */
        MILLIS,

        /**
         * Microseconds.
         */
        MICROS,

        /**
         * Nanoseconds.
         */
        NANOS;

        /**
         * Number of microseconds in a millisecond.
         */
        private static final BigInteger MILLIS_PER_MICROS = BigInteger.valueOf(1000);

        /**
         * Number of milliseconds in a nanosecond.
         */
        private static final BigInteger MILLIS_PER_NANOS = BigInteger.valueOf(1000000);

        /**
         * Number of nanoseconds in a microsecond.
         */
        private static final BigInteger MICROS_PER_NANOS = BigInteger.valueOf(1000);

        /**
         * Takes a value in milliseconds, microseconds or nanoseconds and
         * converts it to the same amount of time in milliseconds, microseconds or nanoseconds.
         *
         * @param value Amount of time
         * @param from  Unit time is currently in
         * @param to    Unit time should be changed to
         * @return Same amount of time in new unit value
         */
        public static BigInteger convert(final BigInteger value, @NonNull final Seconds from, @NonNull final Seconds to) {
            if (value == null) {
                return null;
            }
            if (from == to) {
                return value;
            }
            if (from == MILLIS) {
                if (to == MICROS) {
                    return value.multiply(MILLIS_PER_MICROS);
                } else {
                    return value.multiply(MILLIS_PER_NANOS);
                }
            } else if (from == MICROS) {
                if (to == MILLIS) {
                    return value.divide(MILLIS_PER_MICROS);
                } else {
                    return value.multiply(MICROS_PER_NANOS);
                }
            } else {
                if (to == MILLIS) {
                    return value.divide(MILLIS_PER_NANOS);
                } else {
                    return value.divide(MICROS_PER_NANOS);
                }
            }
        }
    }
}
