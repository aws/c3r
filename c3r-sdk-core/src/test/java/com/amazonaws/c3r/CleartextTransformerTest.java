// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r;

import com.amazonaws.c3r.exception.C3rRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CleartextTransformerTest {

    private CleartextTransformer transformer;

    @BeforeEach
    public void setup() {
        transformer = new CleartextTransformer();
    }

    @Test
    public void marshalTest() {
        final byte[] cleartext = "Some cleartext".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(cleartext, transformer.marshal(cleartext, null));
    }

    @Test
    public void marshalledValueTooLongTest() {
        final byte[] cleartext = new byte[Transformer.MAX_GLUE_STRING_BYTES + 1];
        assertThrows(C3rRuntimeException.class, () -> transformer.marshal(cleartext, null));
    }

    @Test
    public void marshalNullTest() {
        assertNull(transformer.marshal(null, null));
    }

    @Test
    public void unmarshalTest() {
        final byte[] ciphertext = "Some ciphertext".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(ciphertext, transformer.unmarshal(ciphertext));
    }

    @Test
    public void unmarshalNullTest() {
        assertNull(transformer.unmarshal(null));
    }

    @Test
    public void getEncryptionDescriptorTest() {
        assertThrows(C3rRuntimeException.class, () -> transformer.getEncryptionDescriptor());
    }

    @Test
    public void getVersionImmutableTest() {
        final byte[] version = transformer.getVersion();
        Arrays.fill(version, (byte) 0);
        assertFalse(Arrays.equals(transformer.getVersion(), version));
    }
}
