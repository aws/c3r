// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption.keys;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.internal.Nonce;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.amazonaws.c3r.encryption.keys.KeyUtil.SHARED_SECRET_KEY_BYTE_LENGTH;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DerivedEncryptionKeyTest {
    private final byte[] salt = "saltybytes".getBytes(StandardCharsets.UTF_8);

    private final SecretKey secretKey =
            new SecretKeySpec(GeneralTestUtility.EXAMPLE_KEY_BYTES, 0, SHARED_SECRET_KEY_BYTE_LENGTH, KeyUtil.KEY_ALG);

    private final DerivedRootEncryptionKey derivedRootEncryptionKey = new DerivedRootEncryptionKey(secretKey, salt);

    @Test
    public void deriveEncryptionKeyTest() {
        final DerivedEncryptionKey derivedEncryptionKey = new DerivedEncryptionKey(derivedRootEncryptionKey, Nonce.nextNonce());
        assertEquals(KeyUtil.KEY_ALG, derivedEncryptionKey.getAlgorithm());
        assertEquals(SHARED_SECRET_KEY_BYTE_LENGTH, derivedEncryptionKey.getEncoded().length);
        assertFalse(Arrays.equals(derivedRootEncryptionKey.getEncoded(), derivedEncryptionKey.getEncoded()));
    }

    @Test
    public void deriveEncryptionKeySmallKeyTest() {
        final DerivedRootEncryptionKey derivedRootEncryptionKey = mock(DerivedRootEncryptionKey.class);
        when(derivedRootEncryptionKey.getEncoded()).thenReturn(new byte[SHARED_SECRET_KEY_BYTE_LENGTH - 1]); // less than required
        assertThrows(C3rIllegalArgumentException.class, () -> new DerivedEncryptionKey(derivedRootEncryptionKey, Nonce.nextNonce()));
    }

    @Test
    public void deriveEncryptionKeyWithSameNonceTest() {
        final Nonce nonce = Nonce.nextNonce();
        final DerivedEncryptionKey derivedEncryptionKey1 = new DerivedEncryptionKey(derivedRootEncryptionKey, nonce);
        final DerivedEncryptionKey derivedEncryptionKey2 = new DerivedEncryptionKey(derivedRootEncryptionKey, nonce);
        assertArrayEquals(derivedEncryptionKey1.getEncoded(), derivedEncryptionKey2.getEncoded());
    }

    @Test
    public void deriveEncryptionKeyChangesWithNonceTest() {
        final DerivedEncryptionKey derivedEncryptionKey1 = new DerivedEncryptionKey(derivedRootEncryptionKey, Nonce.nextNonce());
        final DerivedEncryptionKey derivedEncryptionKey2 = new DerivedEncryptionKey(derivedRootEncryptionKey, Nonce.nextNonce());
        assertFalse(Arrays.equals(derivedEncryptionKey1.getEncoded(), derivedEncryptionKey2.getEncoded()));
    }

    @Test
    public void deriveEncryptionKeyNullNonceTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> new DerivedEncryptionKey(derivedRootEncryptionKey, null));
    }

    @Test
    public void deriveEncryptionKeyNullRootKeyTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> new DerivedEncryptionKey(null, Nonce.nextNonce()));
    }
}
