// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.data.ClientDataType;
import com.amazonaws.c3r.encryption.EncryptionContext;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FingerprintTransformerTest {
    private final EncryptionContext differentColumnContext = EncryptionContext.builder()
            .columnLabel("differentLabel")
            .clientDataType(ClientDataType.STRING)
            .build();

    private final byte[] secret = "SomeFakeSecretKey".getBytes(StandardCharsets.UTF_8);

    private final byte[] salt = "saltybytes".getBytes(StandardCharsets.UTF_8);

    private final SecretKey secretKey = new SecretKeySpec(secret, 0, secret.length, "AES");

    private final EncryptionContext context = EncryptionContext.builder()
            .columnLabel("label")
            .clientDataType(ClientDataType.STRING)
            .build();

    private FingerprintTransformer fingerprintTransformer;

    @BeforeEach
    public void setup() {
        fingerprintTransformer = new FingerprintTransformer(secretKey, salt, ClientSettings.highAssuranceMode(), false);
    }

    @Test
    public void fingerprintTransformerNullSecretKeyTest() {
        assertThrows(C3rRuntimeException.class,
                () -> new FingerprintTransformer(null, salt, ClientSettings.highAssuranceMode(), false));
    }

    @Test
    public void fingerprintTransformerNullSaltTest() {
        assertThrows(C3rRuntimeException.class,
                () -> new FingerprintTransformer(secretKey, null, ClientSettings.highAssuranceMode(),
                false));
    }

    @Test
    public void fingerprintTransformerEmptySaltTest() {
        assertThrows(C3rRuntimeException.class, () -> new FingerprintTransformer(secretKey, new byte[0],
                ClientSettings.highAssuranceMode(),
                false));
    }

    @Test
    public void marshalNullEncryptionContextTest() {
        final byte[] cleartext = "some cleartext data".getBytes(StandardCharsets.UTF_8);
        assertThrows(C3rIllegalArgumentException.class, () -> fingerprintTransformer.marshal(cleartext, null));
    }

    private byte[] getDescriptorPrefix(final byte[] bytes) {
        return Arrays.copyOfRange(bytes, 0, FingerprintTransformer.DESCRIPTOR_PREFIX.length);
    }

    @Test
    public void marshalAllowJoinsOnColumnsWithDifferentNamesFalseTest() {
        final byte[] cleartext = "some cleartext data".getBytes(StandardCharsets.UTF_8);
        final byte[] hmacText = fingerprintTransformer.marshal(cleartext, context);

        assertArrayEquals(
                FingerprintTransformer.DESCRIPTOR_PREFIX,
                getDescriptorPrefix(hmacText));

        final byte[] expectedText = "02:hmac:WRtZMlbn+zFNU5YAR0UT1S9v128kUMhV2PAJSdjGzqw=".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(expectedText, hmacText);

        final byte[] differentColumnHmacText = fingerprintTransformer.marshal(cleartext, differentColumnContext);
        assertFalse(Arrays.equals(hmacText, differentColumnHmacText));
    }

    @Test
    public void marshalAllowJoinsOnColumnsWithDifferentNamesTrueTest() {
        final FingerprintTransformer fingerprintTransformer = new FingerprintTransformer(secretKey, salt, ClientSettings.lowAssuranceMode(),
                false);
        final byte[] cleartext = "some cleartext data".getBytes(StandardCharsets.UTF_8);
        final byte[] hmacText = fingerprintTransformer.marshal(cleartext, context);
        assertArrayEquals(
                FingerprintTransformer.DESCRIPTOR_PREFIX,
                getDescriptorPrefix(hmacText));

        final byte[] expectedText = "02:hmac:7YchN4H6pV0CxfGNX51VXRjCNx0/V43fMkV0lkTfAeE=".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(expectedText, hmacText);

        final byte[] differentColumnHmacText = fingerprintTransformer.marshal(cleartext, differentColumnContext);
        assertArrayEquals(hmacText, differentColumnHmacText);
    }

    @Test
    public void marshalNullDataPreserveNullsFalseTest() {
        final byte[] hmacText = fingerprintTransformer.marshal(null, context);
        assertNotNull(hmacText);
    }

    @Test
    public void marshalNullDataPreserveNullsTrueTest() {
        fingerprintTransformer = new FingerprintTransformer(secretKey, salt, ClientSettings.lowAssuranceMode(), false);
        final byte[] hmacText = fingerprintTransformer.marshal(null, context);
        assertNull(hmacText);
    }

    @Test
    public void marshalEmptyDataTest() {
        final byte[] cleartext = "".getBytes(StandardCharsets.UTF_8);
        final byte[] hmacText = fingerprintTransformer.marshal(cleartext, context);
        assertArrayEquals(
                FingerprintTransformer.DESCRIPTOR_PREFIX,
                getDescriptorPrefix(hmacText));

        final byte[] expectedText = "02:hmac:+UJjyBG1kJXUe4u0C5FTM7WaEhGl9+PB5blhAURDvnQ=".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(expectedText, hmacText);
    }

    @Test
    public void unmarshalFailOnUnmarshalTrueTest() {
        fingerprintTransformer = new FingerprintTransformer(secretKey, salt, ClientSettings.lowAssuranceMode(), true);
        final byte[] hmacText = "02:hmac:i0Y63cL+J5DpQw3rd3lnnwT1LSBEv+MppUxrajPkz44=".getBytes(StandardCharsets.UTF_8);
        assertThrows(C3rRuntimeException.class, () -> fingerprintTransformer.unmarshal(hmacText));
    }

    @Test
    public void unmarshalFailOnUnmarshalFalseTest() {
        fingerprintTransformer = new FingerprintTransformer(secretKey, salt, ClientSettings.lowAssuranceMode(), false);
        final byte[] hmacText = "01:hmac:i0Y63cL+J5DpQw3rd3lnnwT1LSBEv+MppUxrajPkz44=".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(hmacText, fingerprintTransformer.unmarshal(hmacText));
    }

    @Test
    public void getEncryptionDescriptorImmutableTest() {
        final byte[] descriptor = fingerprintTransformer.getEncryptionDescriptor();
        Arrays.fill(descriptor, (byte) 0);
        assertFalse(Arrays.equals(fingerprintTransformer.getEncryptionDescriptor(), descriptor));
    }

    @Test
    public void getVersionImmutableTest() {
        final byte[] version = fingerprintTransformer.getVersion();
        Arrays.fill(version, (byte) 0);
        assertFalse(Arrays.equals(fingerprintTransformer.getVersion(), version));
    }

    @Test
    public void descriptorStringTest() {
        assertTrue(Transformer.hasDescriptor(
                fingerprintTransformer,
                FingerprintTransformer.DESCRIPTOR_PREFIX_STRING.getBytes(StandardCharsets.UTF_8)));
    }
}
