// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption.keys;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;

import javax.crypto.SecretKey;

/**
 * The SaltedHkdf is a wrapper on the HmacKeyDerivationFunction class, exposing only the deriveKey function call.
 * HmacKeyDerivationFunction is pulled from the AWS Encryption SDK and is otherwise unmodified, but provides more functionality than
 * desirable.
 */
public class SaltedHkdf {
    /**
     * The algorithm to use to derive keys.
     */
    private final HmacKeyDerivationFunction hkdf;

    /**
     * Creates the HKDF to be used for root key derivation or HMACed columns. Note that this key and salt pair must be shared across all
     * members of the collaboration.
     *
     * @param secretKey The key to be used to initialize the HKDF
     * @param salt      The salt to be used to initialize the HKDF
     * @throws C3rIllegalArgumentException If the key and salt aren't valid
     * @throws C3rRuntimeException         If the root key couldn't be created
     */
    public SaltedHkdf(final SecretKey secretKey, final byte[] salt) {
        if (secretKey == null) {
            throw new C3rIllegalArgumentException("A SecretKey must be provided when deriving the root encryption key, but was null.");
        }
        if (salt == null || salt.length == 0) {
            throw new C3rIllegalArgumentException("A salt must be provided when deriving the root encryption key, but was null or empty.");
        }
        hkdf = HmacKeyDerivationFunction.getInstance(KeyUtil.HMAC_ALG);
        hkdf.init(secretKey.getEncoded(), salt);
    }

    /**
     * Derive key from using extra randomness added by {@code info}.
     *
     * @param info   Extra randomness the application can optionally add during key generation
     * @param length Length of generated key
     * @return Pseudorandom key of {@code length} bytes
     */
    public byte[] deriveKey(final byte[] info, final int length) {
        return hkdf.deriveKey(info, length);
    }
}
