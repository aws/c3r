// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class UnitsTest {
    private static Stream<Arguments> secondConversions() {
        return Stream.of(
                Arguments.of(2L, 2L, Units.Seconds.MILLIS, Units.Seconds.MILLIS),
                Arguments.of(2L, 2000L, Units.Seconds.MILLIS, Units.Seconds.MICROS),
                Arguments.of(2L, 2000000L, Units.Seconds.MILLIS, Units.Seconds.NANOS),
                Arguments.of(3000L, 3L, Units.Seconds.MICROS, Units.Seconds.MILLIS),
                Arguments.of(3L, 3L, Units.Seconds.MICROS, Units.Seconds.MICROS),
                Arguments.of(3L, 3000L, Units.Seconds.MICROS, Units.Seconds.NANOS),
                Arguments.of(4000000L, 4L, Units.Seconds.NANOS, Units.Seconds.MILLIS),
                Arguments.of(4000L, 4L, Units.Seconds.NANOS, Units.Seconds.MICROS),
                Arguments.of(4L, 4L, Units.Seconds.NANOS, Units.Seconds.NANOS)
        );
    }

    @ParameterizedTest
    @MethodSource("secondConversions")
    public void secondConversionTest(final Long value, final Long result, final Units.Seconds from, final Units.Seconds to) {
        assertEquals(BigInteger.valueOf(result), Units.Seconds.convert(BigInteger.valueOf(value), from, to));
    }

    @Test
    public void nullValueSecondConversionTest() {
        assertNull(Units.Seconds.convert(null, Units.Seconds.MICROS, Units.Seconds.MICROS));
    }
}
