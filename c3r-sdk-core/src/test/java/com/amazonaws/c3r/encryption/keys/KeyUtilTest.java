// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption.keys;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

import static com.amazonaws.c3r.encryption.keys.KeyUtil.SHARED_SECRET_KEY_BYTE_LENGTH;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KeyUtilTest {

    @Test
    public void getAes256KeyFromEnvVarTest() {
        final String keyMaterial = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
        final String secretKeyBase64 = Base64.getEncoder().encodeToString(GeneralTestUtility.EXAMPLE_KEY_BYTES);
        assertEquals(secretKeyBase64, keyMaterial);
        final byte[] keyBytes = Base64.getDecoder().decode(keyMaterial);
        assertArrayEquals(GeneralTestUtility.EXAMPLE_KEY_BYTES, keyBytes);
        final SecretKey secretKey =
                new SecretKeySpec(GeneralTestUtility.EXAMPLE_KEY_BYTES, 0, SHARED_SECRET_KEY_BYTE_LENGTH, KeyUtil.KEY_ALG);
        assertEquals(secretKey, KeyUtil.sharedSecretKeyFromString(keyMaterial));
    }

    @Test
    public void getAes256KeyFromEnvVarMissingTest() {
        final String keyMaterial = System.getenv(KeyUtil.KEY_ENV_VAR);
        assertNull(keyMaterial);
        assertThrows(C3rIllegalArgumentException.class, () -> KeyUtil.sharedSecretKeyFromString(null));
    }

    @Test
    public void getAes256KeyFromEnvVarTooShortTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> KeyUtil.sharedSecretKeyFromString("12345"));
    }

    @Test
    public void getAes256KeyFromEnvVarLongerThanNecessaryTest() {
        final String keyMaterial = "11111111222222223333333344444444555555556666666677777777";
        final byte[] expected = Base64.getDecoder().decode(keyMaterial);
        assertArrayEquals(expected, KeyUtil.sharedSecretKeyFromString(keyMaterial).getEncoded());
    }
}
