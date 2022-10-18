// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption.keys;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

import static com.amazonaws.c3r.encryption.keys.KeyUtil.SHARED_SECRET_KEY_BYTE_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DerivedRootEncryptionKeyTest {
    private final byte[] salt = "saltybytes".getBytes(StandardCharsets.UTF_8);

    private final SecretKey secretKey =
            new SecretKeySpec(GeneralTestUtility.EXAMPLE_KEY_BYTES, 0, SHARED_SECRET_KEY_BYTE_LENGTH, KeyUtil.KEY_ALG);

    @Test
    public void deriveRootEncryptionKeyTest() {
        final DerivedRootEncryptionKey derivedRootEncryptionKey = new DerivedRootEncryptionKey(secretKey, salt);

        assertEquals(KeyUtil.KEY_ALG, derivedRootEncryptionKey.getAlgorithm());
        assertEquals(SHARED_SECRET_KEY_BYTE_LENGTH, derivedRootEncryptionKey.getEncoded().length);
    }

    @Test
    public void deriveRootEncryptionKeyNullKeyTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> new DerivedRootEncryptionKey(null, salt));
    }

    @Test
    public void deriveRootEncryptionKeyNullSaltTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> new DerivedRootEncryptionKey(secretKey, null));
    }

    @Test
    public void deriveRootEncryptionKeyEmptySaltTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> new DerivedRootEncryptionKey(secretKey, new byte[0]));
    }

}
