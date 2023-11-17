// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static com.amazonaws.c3r.data.ClientDataInfo.FLAG_COUNT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientDataInfoTest {
    @Test
    public void validateWithinBitWidthLimit() {
        // Ensure we stay within 8 bits.
        assertTrue(FLAG_COUNT + ClientDataType.BITS <= ClientDataInfo.BYTE_LENGTH * 8);
    }

    @ParameterizedTest
    @EnumSource(ClientDataType.class)
    public void encodeDecodeStringNonNull(final ClientDataType type) {
        final var nullUnknown = ClientDataInfo.builder().type(ClientDataType.UNKNOWN).isNull(true).build();
        final var nonNullUnknown = ClientDataInfo.builder().type(ClientDataType.UNKNOWN).isNull(false).build();

        if (type != ClientDataType.UNKNOWN) {
            final var nullType = ClientDataInfo.builder().type(type).isNull(true).build();
            final var nonNullType = ClientDataInfo.builder().type(type).isNull(false).build();

            // Test round trip encode/decode remains the same
            assertEquals(nullType, ClientDataInfo.decode(nullType.encode()));
            assertEquals(nonNullType, ClientDataInfo.decode(nonNullType.encode()));

            // Make sure decoded type is unique from other type
            assertNotEquals(ClientDataInfo.decode(nullType.encode()), nullUnknown);
            assertNotEquals(ClientDataInfo.decode(nonNullType.encode()), nonNullUnknown);

            // Make sure null status is kept through encode/decode
            assertNotEquals(ClientDataInfo.decode(nullType.encode()), ClientDataInfo.decode(nonNullType.encode()));

            // Make sure decoded opposite null with different type doesn't match
            assertNotEquals(ClientDataInfo.decode(nullType.encode()), nonNullUnknown);
            assertNotEquals(ClientDataInfo.decode(nonNullType.encode()), nullUnknown);
        } else {
            // Special case for ClientDataType.UNKNOWN since it needs to be compared against a different value.
            final var nullString = ClientDataInfo.builder().type(ClientDataType.STRING).isNull(true).build();
            final var nonNullString = ClientDataInfo.builder().type(ClientDataType.STRING).isNull(false).build();

            // Test round trip encode/decode remains the same
            assertEquals(nullUnknown, ClientDataInfo.decode(nullUnknown.encode()));
            assertEquals(nonNullUnknown, ClientDataInfo.decode(nonNullUnknown.encode()));

            // Make sure decoded type is unique from other type
            assertNotEquals(ClientDataInfo.decode(nullUnknown.encode()), nullString);
            assertNotEquals(ClientDataInfo.decode(nonNullUnknown.encode()), nonNullString);

            // Make sure null status is kept through encode/decode
            assertNotEquals(ClientDataInfo.decode(nullUnknown.encode()), ClientDataInfo.decode(nonNullUnknown.encode()));

            // Make sure decoded opposite null with different type doesn't match
            assertNotEquals(ClientDataInfo.decode(nullUnknown.encode()), nonNullString);
            assertNotEquals(ClientDataInfo.decode(nonNullUnknown.encode()), nullString);
        }
    }
}
