// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption.keys;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.internal.Nonce;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class KeyTest {
    @Test
    public void nullKeyValidationTest() {
        final byte[] salt = "saltybytes".getBytes(StandardCharsets.UTF_8);
        assertThrows(C3rIllegalArgumentException.class, () -> new DerivedRootEncryptionKey(null, salt));
        assertThrows(C3rIllegalArgumentException.class, () -> new DerivedEncryptionKey(null, Nonce.nextNonce()));
    }
}
