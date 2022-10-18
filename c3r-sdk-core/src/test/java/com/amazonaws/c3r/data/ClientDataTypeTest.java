// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientDataTypeTest {

    @Test
    public void validateValueCountTest() {
        // Make sure the number of supported types fits in our allocated 6 bits for encoding
        assertTrue(ClientDataType.values().length <= ClientDataType.MAX_DATATYPE_COUNT);
    }

    @Test
    public void validateDataTypeIndicesTest() {
        // Make sure there aren't any duplicate values
        final var uniqueIndexCount = Arrays.stream(ClientDataType.values()).map(ClientDataType::getIndex)
                .collect(Collectors.toSet())
                .size();
        assertEquals(uniqueIndexCount, ClientDataType.values().length);
        // Ensure each individual value fits within the reserved number of bits
        for (ClientDataType t : ClientDataType.values()) {
            assertTrue(t.getIndex() <= ClientDataType.MAX_DATATYPE_COUNT);
        }
    }

    @Test
    public void stringIndexTest() {
        assertEquals(
                ClientDataType.STRING,
                ClientDataType.fromIndex(ClientDataType.STRING.getIndex()));
    }

    @Test
    public void unknownIndexTest() {
        assertEquals(
                ClientDataType.UNKNOWN,
                ClientDataType.fromIndex(ClientDataType.UNKNOWN.getIndex()));
    }

    @Test
    public void invalidIndexTest() {
        assertThrows(C3rIllegalArgumentException.class, () ->
                ClientDataType.fromIndex((byte) -1));
        assertThrows(C3rIllegalArgumentException.class, () ->
                ClientDataType.fromIndex((byte) ClientDataType.values().length));
    }

}
