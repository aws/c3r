// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption;

import com.amazonaws.c3r.encryption.providers.SymmetricStaticProvider;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.AdditionalAuthenticatedData;
import com.amazonaws.c3r.internal.InitializationVector;
import com.amazonaws.c3r.internal.Nonce;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class EncryptorTest {

    private final AdditionalAuthenticatedData aad = new AdditionalAuthenticatedData(new byte[]{1});

    private final Nonce nonce = new Nonce("nonce01234567890nonce01234567890".getBytes(StandardCharsets.UTF_8));

    private final byte[] secret = "SomeFakeSecretKey".getBytes(StandardCharsets.UTF_8);

    private final byte[] salt = "saltybytes".getBytes(StandardCharsets.UTF_8);

    private final SecretKey secretKey = new SecretKeySpec(secret, 0, secret.length, "AES");

    private final InitializationVector iv = InitializationVector.deriveIv("label", nonce);

    private final EncryptionContext context = EncryptionContext.builder()
            .nonce(nonce)
            .columnLabel("label")
            .build();

    @Test
    public void encryptionWithSymmetricStaticProviderTest() {
        final SymmetricStaticProvider provider = new SymmetricStaticProvider(secretKey, salt);
        final Encryptor encryptor = Encryptor.getInstance(provider);
        final byte[] cleartext = "some plain text".getBytes(StandardCharsets.UTF_8);
        final byte[] cipherText = encryptor.encrypt(cleartext, iv, aad, context);

        assertNotEquals(cleartext, cipherText);

        final byte[] decryptedText = encryptor.decrypt(cipherText, iv, aad, context);

        assertArrayEquals(cleartext, decryptedText);
    }

    @Test
    public void encryptionWithSymmetricStaticProviderAndNullDataTest() {
        final SymmetricStaticProvider provider = new SymmetricStaticProvider(secretKey, salt);
        final Encryptor encryptor = Encryptor.getInstance(provider);

        assertThrows(C3rIllegalArgumentException.class, () -> encryptor.encrypt(null, iv, aad, context));
    }

    @Test
    public void encryptionWithSymmetricStaticProviderAndNullAadTest() {
        final SymmetricStaticProvider provider = new SymmetricStaticProvider(secretKey, salt);
        final Encryptor encryptor = Encryptor.getInstance(provider);
        final EncryptionContext context = EncryptionContext.builder()
                .nonce(nonce)
                .columnLabel("label")
                .build();
        final byte[] cleartext = "some plain text".getBytes(StandardCharsets.UTF_8);
        final byte[] cipherText = encryptor.encrypt(cleartext, iv, null, context);

        assertNotEquals(cleartext, cipherText);

        final byte[] decryptedText = encryptor.decrypt(cipherText, iv, null, context);

        assertArrayEquals(cleartext, decryptedText);
    }

    @Test
    public void encryptionWithSymmetricStaticProviderAndNullNonceTest() {
        final SymmetricStaticProvider provider = new SymmetricStaticProvider(secretKey, salt);
        final Encryptor encryptor = Encryptor.getInstance(provider);
        final EncryptionContext context = EncryptionContext.builder()
                .nonce(null)
                .columnLabel("label")
                .build();
        final byte[] cleartext = "some plain text".getBytes(StandardCharsets.UTF_8);
        assertThrows(C3rIllegalArgumentException.class, () -> encryptor.encrypt(cleartext, iv, aad, context));
    }

    @Test
    public void encryptionWithSymmetricStaticProviderAndNullEncryptionContextTest() {
        final SymmetricStaticProvider provider = new SymmetricStaticProvider(secretKey, salt);
        final Encryptor encryptor = Encryptor.getInstance(provider);
        final byte[] cleartext = "some plain text".getBytes(StandardCharsets.UTF_8);
        assertThrows(C3rIllegalArgumentException.class, () -> encryptor.encrypt(cleartext, iv, aad, null));
    }

    @Test
    public void encryptionWithSymmetricStaticProviderAndBadKeyMaterialsTest() {
        final SymmetricStaticProvider provider = new SymmetricStaticProvider(secretKey, salt);
        final Encryptor encryptor = Encryptor.getInstance(provider);
        final byte[] cleartext = "some plain text".getBytes(StandardCharsets.UTF_8);
        final byte[] cipherText = encryptor.encrypt(cleartext, iv, aad, context);

        assertNotEquals(cleartext, cipherText);

        final byte[] badSecret = "SomeBadKey".getBytes(StandardCharsets.UTF_8);
        final SecretKey badKey = new SecretKeySpec(badSecret, 0, badSecret.length, "AES");
        final Encryptor badEncryptor = Encryptor.getInstance(new SymmetricStaticProvider(badKey, salt));

        assertThrows(C3rRuntimeException.class, () -> badEncryptor.decrypt(cipherText, iv, aad, context));
    }

    @Test
    public void encryptionWithSymmetricStaticProviderAndBadSaltTest() {
        final SymmetricStaticProvider provider = new SymmetricStaticProvider(secretKey, salt);
        final Encryptor encryptor = Encryptor.getInstance(provider);
        final byte[] cleartext = "some plain text".getBytes(StandardCharsets.UTF_8);
        final byte[] cipherText = encryptor.encrypt(cleartext, iv, aad, context);

        assertNotEquals(cleartext, cipherText);

        final byte[] badSalt = "AnIncorrectSalt".getBytes(StandardCharsets.UTF_8);
        final Encryptor badEncryptor = Encryptor.getInstance(new SymmetricStaticProvider(secretKey, badSalt));

        assertThrows(C3rRuntimeException.class, () -> badEncryptor.decrypt(cipherText, iv, aad, context));
    }
}
