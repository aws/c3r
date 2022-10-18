// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.encryption.Encryptor;
import com.amazonaws.c3r.encryption.keys.KeyUtil;
import com.amazonaws.c3r.encryption.providers.SymmetricStaticProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransformerTest {
    private Transformer transformer;

    @BeforeEach
    public void setup() {
        final byte[] secret = "SomeFakeSecretKey".getBytes(StandardCharsets.UTF_8);
        final byte[] salt = "saltybytes".getBytes(StandardCharsets.UTF_8);
        final SecretKey secretKey = new SecretKeySpec(secret, 0, secret.length, KeyUtil.KEY_ALG);
        final SymmetricStaticProvider provider = new SymmetricStaticProvider(secretKey, salt);
        transformer = new SealedTransformer(Encryptor.getInstance(provider), ClientSettings.highAssuranceMode());
    }

    @Test
    public void hasDescriptorTest() {
        final ByteBuffer value = ByteBuffer.allocate(transformer.getVersion().length + transformer.getEncryptionDescriptor().length)
                .put(transformer.getVersion())
                .put(transformer.getEncryptionDescriptor());
        assertTrue(Transformer.hasDescriptor(transformer, value.array()));
    }

    @Test
    public void hasDescriptorEmptyTest() {
        assertFalse(Transformer.hasDescriptor(transformer, new byte[0]));
    }

    @Test
    public void hasDescriptorTooShortTest() {
        // Not enough room for descriptor
        assertFalse(Transformer.hasDescriptor(transformer, new byte[transformer.getVersion().length]));
    }

    @Test
    public void hasDescriptorNotPresentTest() {
        // Descriptor not present
        final byte[] value = new byte[100];
        Arrays.fill(value, (byte) 0);
        assertFalse(Transformer.hasDescriptor(transformer, value));
    }
}
