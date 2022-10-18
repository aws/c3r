// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption.keys;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.Nonce;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * This class is a wrapper on SecretKeys. It adds type safety for ensuring that only a DerivedEncryptionKey is used for encrypting
 * individual data.
 */
public class DerivedEncryptionKey extends Key {
    /**
     * Sets up the key with a shared secret along with a nonce to add randomness.
     *
     * @param derivedRootEncryptionKey Shared secret used to generate symmetric key
     * @param nonce                    Adds extra randomness
     */
    public DerivedEncryptionKey(final DerivedRootEncryptionKey derivedRootEncryptionKey, final Nonce nonce) {
        super(deriveEncryptionKey(derivedRootEncryptionKey, nonce));
    }

    /**
     * Derives new encryption key from the root encryption key and nonce.
     *
     * @param derivedRootEncryptionKey The secret key to derive a new key from
     * @param nonce                    a random byte sequence
     * @return a newly derived encryption key
     * @throws C3rIllegalArgumentException If the key material doesn't meet requirements
     * @throws C3rRuntimeException         If the algorithm requested is not available
     */
    private static SecretKey deriveEncryptionKey(final DerivedRootEncryptionKey derivedRootEncryptionKey, final Nonce nonce) {
        if (derivedRootEncryptionKey == null) {
            throw new C3rIllegalArgumentException("A root key must be provided when deriving an encryption key, but was null.");
        } else if (nonce == null) {
            throw new C3rIllegalArgumentException("A nonce must be provided when deriving an encryption key, but was null.");
        }
        final byte[] key = derivedRootEncryptionKey.getEncoded();
        if (key.length != KeyUtil.SHARED_SECRET_KEY_BYTE_LENGTH) {
            throw new C3rIllegalArgumentException("A root key must be 32 bytes, but was " + key.length + " bytes.");
        }
        final byte[] nonceBytes = nonce.getBytes();
        final ByteBuffer buffer = ByteBuffer.allocate(key.length + nonceBytes.length);
        buffer.put(key);
        buffer.put(nonceBytes);
        final MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance(KeyUtil.HASH_ALG);
        } catch (NoSuchAlgorithmException e) {
            throw new C3rRuntimeException("Requested algorithm `" + KeyUtil.HASH_ALG + "` is not available!", e);
        }
        return new SecretKeySpec(messageDigest.digest(buffer.array()), KeyUtil.KEY_ALG);
    }
}
