// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption.keys;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

/**
 * Utility class for key management and general key information.
 */
public abstract class KeyUtil {
    /**
     * Key generation algorithm.
     */
    public static final String KEY_ALG = "AES";

    /**
     * Environment variable shared secret should be stored in for CLI users.
     */
    public static final String KEY_ENV_VAR = "C3R_SHARED_SECRET";

    /**
     * Algorithm to use for HMAC.
     */
    public static final String HMAC_ALG = "HmacSHA256";

    /**
     * Application specific context to add randomness to key generation.
     */
    public static final String HKDF_INFO = "c3r-encryption-primary-aes-gcm-256";

    /**
     * Application specific context to add randomness to key generation.
     */
    public static final String HKDF_COLUMN_BASED_INFO = "c3r-hmac-sha256-col-";

    /**
     * Algorithm to use for hashing.
     */
    public static final String HASH_ALG = "SHA-256";

    /**
     * How long in bytes the shared secret should be.
     */
    public static final int SHARED_SECRET_KEY_BYTE_LENGTH = 32;

    /**
     * Help message for how the CLI users should pass in the shared secret.
     */
    private static final String CLI_ENV_VAR_MESSAGE =
            "(CLI users should inspect the value passed via the `" + KEY_ENV_VAR + "` environment variable.)";

    /**
     * Construct a {@code SecretKey} from the Base64 encoded contents of the string.,
     * checking its size is greater than or equal to {@value #SHARED_SECRET_KEY_BYTE_LENGTH} bytes.
     *
     * @param base64EncodedKeyMaterial Base64-encoded shared secret key
     * @return A SecretKey containing the bytes read from the environment variable
     * @throws C3rIllegalArgumentException If the key material is invalid
     */
    public static SecretKey sharedSecretKeyFromString(final String base64EncodedKeyMaterial) {
        if (base64EncodedKeyMaterial == null) {
            throw new C3rIllegalArgumentException("Shared secret key was null. " + CLI_ENV_VAR_MESSAGE);
        }
        final byte[] keyMaterial;
        try {
            keyMaterial = Base64.getDecoder().decode(base64EncodedKeyMaterial);
        } catch (IllegalArgumentException e) {
            throw new C3rIllegalArgumentException(
                    "Shared secret key could not be decoded from Base64. Please verify that the key material is encoded as Base64. "
                            + CLI_ENV_VAR_MESSAGE, e);
        }
        if (keyLengthIsValid(keyMaterial.length)) {
            return new SecretKeySpec(keyMaterial, KEY_ALG);
        } else {
            throw new C3rIllegalArgumentException("Shared secret key was expected to have at least "
                    + SHARED_SECRET_KEY_BYTE_LENGTH + " bytes, but was found to contain " +
                    keyMaterial.length + " bytes. "
                    + CLI_ENV_VAR_MESSAGE);
        }
    }

    /**
     * Ensure key is at least {@value #SHARED_SECRET_KEY_BYTE_LENGTH} bytes long.
     *
     * @param byteLength Actual key length
     * @return {@code true} if key is at least as long as required bytes
     */
    private static boolean keyLengthIsValid(final long byteLength) {
        return byteLength >= SHARED_SECRET_KEY_BYTE_LENGTH;
    }

}
