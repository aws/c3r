// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import org.junit.jupiter.api.Test;

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

    public void validateEncodeDecode(final ClientDataType type, final ClientDataType otherType, final boolean isNull) {
        final var info = ClientDataInfo.builder().type(type).isNull(isNull).build();
        assertEquals(
                info,
                ClientDataInfo.decode(info.encode()));
        assertNotEquals(
                ClientDataInfo.decode(info.encode()),
                ClientDataInfo.builder().type(otherType).isNull(isNull).build());
        assertNotEquals(
                ClientDataInfo.decode(info.encode()),
                ClientDataInfo.builder().type(type).isNull(!isNull).build());
        assertNotEquals(
                ClientDataInfo.decode(info.encode()),
                ClientDataInfo.builder().type(otherType).isNull(!isNull).build());
    }

    @Test
    public void encodeDecodeStringNonNull() {
        validateEncodeDecode(ClientDataType.STRING, ClientDataType.UNKNOWN, false);
    }

    @Test
    public void encodeDecodeStringNull() {
        validateEncodeDecode(ClientDataType.STRING, ClientDataType.UNKNOWN, true);
    }

    @Test
    public void encodeDecodeUnknownNonNull() {
        validateEncodeDecode(ClientDataType.STRING, ClientDataType.UNKNOWN, true);
    }

    @Test
    public void encodeDecodeUnknownNull() {
        validateEncodeDecode(ClientDataType.STRING, ClientDataType.UNKNOWN, true);
    }
}
