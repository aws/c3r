// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption.keys;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

/**
 * This class is a wrapper on SecretKeys. It adds type safety for ensuring that a DerivedRootEncryptionKey is <b>never</b> used for
 * encrypting individual data and that only a DerivedRootEncryptionKey may be used for generating {@link DerivedEncryptionKey} objects.
 */
public class DerivedRootEncryptionKey extends Key {
    /**
     * Creates the key to be used for all future key delegations. Note that this key and salt pair must be shared between all parties.
     *
     * @param secretKey The key to be used to initialize the root key
     * @param salt      The salt to be used to initialize the root key
     */
    public DerivedRootEncryptionKey(final SecretKey secretKey, final byte[] salt) {
        super(deriveRootEncryptionKey(secretKey, salt));
    }

    /**
     * Creates the key to be used for all future key delegations. Note that this key and salt pair must be shared across all members of
     * the collaboration.
     *
     * @param secretKey The key to be used to initialize the root key
     * @param salt      The salt to be used to initialize the root key
     * @return A key derived from the HMAC of the key provided
     */
    private static SecretKey deriveRootEncryptionKey(final SecretKey secretKey, final byte[] salt) {
        final SaltedHkdf hkdf = new SaltedHkdf(secretKey, salt);
        return new SecretKeySpec(hkdf.deriveKey(KeyUtil.HKDF_INFO.getBytes(StandardCharsets.UTF_8),
                KeyUtil.SHARED_SECRET_KEY_BYTE_LENGTH), KeyUtil.KEY_ALG);
    }
}
