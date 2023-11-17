// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.data;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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

    @ParameterizedTest
    @EnumSource(ClientDataType.class)
    public void fromIndexTest(final ClientDataType type) {
        assertEquals(type, ClientDataType.fromIndex(type.getIndex()));
    }

    @Test
    public void invalidIndexTest() {
        assertThrows(C3rIllegalArgumentException.class, () ->
                ClientDataType.fromIndex((byte) -1));
        assertThrows(C3rIllegalArgumentException.class, () ->
                ClientDataType.fromIndex((byte) ClientDataType.values().length));
    }

    @ParameterizedTest
    @EnumSource(value = ClientDataType.class,
            names = {"BIGINT", "INT", "SMALLINT", "STRING", "CHAR", "VARCHAR", "BOOLEAN", "DATE"},
            mode = EnumSource.Mode.EXCLUDE)
    public void invalidTypesTest(final ClientDataType type) {
        assertThrows(C3rRuntimeException.class, () -> type.getRepresentativeType());
    }

    @ParameterizedTest
    @EnumSource(value = ClientDataType.class, names = {"BIGINT", "INT", "SMALLINT"})
    public void validIntegralTypesTest(final ClientDataType type) {
        assertEquals(ClientDataType.BIGINT, type.getRepresentativeType());
    }

    @ParameterizedTest
    @EnumSource(value = ClientDataType.class, names = {"STRING", "CHAR", "VARCHAR"})
    public void validStringTypesTest(final ClientDataType type) {
        assertEquals(ClientDataType.STRING, type.getRepresentativeType());
    }

    @Test
    public void validBooleanTypesTest() {
        assertEquals(ClientDataType.BOOLEAN, ClientDataType.BOOLEAN.getRepresentativeType());
    }

    @Test
    public void validDateTypesTest() {
        assertEquals(ClientDataType.DATE, ClientDataType.DATE.getRepresentativeType());
    }

    @Test
    public void indicesAreUnchangedTest() {
        // Make sure enumeration hasn't changed order so we don't break backward compatibility unknowingly
        assertEquals(ClientDataType.UNKNOWN, ClientDataType.fromIndex((byte) 0));
        assertEquals(ClientDataType.STRING, ClientDataType.fromIndex((byte) 1));
        assertEquals(ClientDataType.BIGINT, ClientDataType.fromIndex((byte) 2));
        assertEquals(ClientDataType.BOOLEAN, ClientDataType.fromIndex((byte) 3));
        assertEquals(ClientDataType.CHAR, ClientDataType.fromIndex((byte) 4));
        assertEquals(ClientDataType.DATE, ClientDataType.fromIndex((byte) 5));
        assertEquals(ClientDataType.DECIMAL, ClientDataType.fromIndex((byte) 6));
        assertEquals(ClientDataType.DOUBLE, ClientDataType.fromIndex((byte) 7));
        assertEquals(ClientDataType.FLOAT, ClientDataType.fromIndex((byte) 8));
        assertEquals(ClientDataType.INT, ClientDataType.fromIndex((byte) 9));
        assertEquals(ClientDataType.SMALLINT, ClientDataType.fromIndex((byte) 10));
        assertEquals(ClientDataType.TIMESTAMP, ClientDataType.fromIndex((byte) 11));
        assertEquals(ClientDataType.VARCHAR, ClientDataType.fromIndex((byte) 12));
    }
}
