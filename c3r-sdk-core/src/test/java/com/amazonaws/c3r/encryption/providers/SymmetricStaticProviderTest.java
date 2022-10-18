// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption.providers;

import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class SymmetricStaticProviderTest {
    private final byte[] secret = "SomeFakeSecretKey".getBytes(StandardCharsets.UTF_8);

    private final SecretKey secretKey = new SecretKeySpec(secret, 0, secret.length, "AES");

    private final byte[] salt = "saltybytes".getBytes(StandardCharsets.UTF_8);

    @Test
    public void constructorTest() {
        final SymmetricStaticProvider provider = new SymmetricStaticProvider(secretKey, salt);
        final byte[] encryptionKey = provider.getEncryptionMaterials(null).getRootEncryptionKey().getEncoded();
        final byte[] decryptionKey = provider.getDecryptionMaterials(null).getRootDecryptionKey().getEncoded();
        assertArrayEquals(encryptionKey, decryptionKey);
    }

    @Test
    public void refreshTest() {
        final SymmetricStaticProvider provider = new SymmetricStaticProvider(secretKey, salt);
        final byte[] encryptionKey = provider.getEncryptionMaterials(null).getRootEncryptionKey().getEncoded();
        final byte[] decryptionKey = provider.getDecryptionMaterials(null).getRootDecryptionKey().getEncoded();

        provider.refresh();
        final byte[] refreshedEncryptionKey = provider.getEncryptionMaterials(null).getRootEncryptionKey().getEncoded();
        final byte[] refreshedDecryptionKey = provider.getDecryptionMaterials(null).getRootDecryptionKey().getEncoded();
        assertArrayEquals(encryptionKey, refreshedEncryptionKey);
        assertArrayEquals(decryptionKey, refreshedDecryptionKey);
    }
}
