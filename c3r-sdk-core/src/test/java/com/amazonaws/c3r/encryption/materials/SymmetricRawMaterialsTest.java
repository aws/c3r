// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption.materials;

import com.amazonaws.c3r.encryption.keys.DerivedRootEncryptionKey;
import org.junit.jupiter.api.Test;

import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class SymmetricRawMaterialsTest {
    private final byte[] secret = "SomeFakeSecretKey".getBytes(StandardCharsets.UTF_8);

    private final byte[] salt = "saltybytes".getBytes(StandardCharsets.UTF_8);

    private final DerivedRootEncryptionKey secretKey =
            new DerivedRootEncryptionKey(new SecretKeySpec(secret, 0, secret.length, "AES"), salt);

    @Test
    public void constructorTest() {
        final SymmetricRawMaterials materials = new SymmetricRawMaterials(secretKey);
        final byte[] encryptionKey = materials.getRootEncryptionKey().getEncoded();
        final byte[] decryptionKey = materials.getRootDecryptionKey().getEncoded();
        assertArrayEquals(encryptionKey, decryptionKey);
    }
}
