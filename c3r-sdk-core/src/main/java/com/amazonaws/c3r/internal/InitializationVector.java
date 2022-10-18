// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.internal;

import com.amazonaws.c3r.encryption.keys.KeyUtil;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * The initialization data that is unique to the data field instance.
 */
public class InitializationVector {
    /**
     * Length of IVs.
     */
    public static final int IV_BYTE_LENGTH = 12;

    /**
     * The IV value.
     */
    private byte[] bytes;

    /**
     * Set the initialization vector to the specified value.
     *
     * @param bytes Value to use as IV
     */
    public InitializationVector(final byte[] bytes) {
        if (bytes != null) {
            this.bytes = bytes.clone();
        }
        validate();
    }

    /**
     * Derives the IV for a given column and nonce.
     *
     * @param label The label of the column the IV is being generated for
     * @param nonce The nonce for the row the IV is being generated for
     * @return an IV unique to the cell to be encrypted
     * @throws C3rIllegalArgumentException If the data provided was incomplete
     * @throws C3rRuntimeException         If the hash algorithm is not available on this system
     */
    public static InitializationVector deriveIv(final String label, final Nonce nonce) {
        if (label == null || label.isBlank()) {
            throw new C3rIllegalArgumentException("A column label must be provided when generating an IV, but was null or empty.");
        }
        if (nonce == null) {
            throw new C3rIllegalArgumentException("A nonce must be provided when generating an IV, but was null.");
        }
        final byte[] labelBytes = label.getBytes(StandardCharsets.UTF_8);
        final byte[] nonceBytes = nonce.getBytes();
        final byte[] buffer = ByteBuffer.allocate(labelBytes.length + nonceBytes.length)
                .put(labelBytes)
                .put(nonceBytes)
                .array();
        final MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance(KeyUtil.HASH_ALG);
        } catch (NoSuchAlgorithmException e) {
            throw new C3rRuntimeException("Requested algorithm `" + KeyUtil.HASH_ALG + "` is not available!", e);
        }
        final byte[] hash = messageDigest.digest(buffer);
        final byte[] iv = new byte[InitializationVector.IV_BYTE_LENGTH];
        if (hash != null) {
            ByteBuffer.wrap(hash).get(iv);
        }
        return new InitializationVector(iv);
    }

    /**
     * Get a copy of the IV.
     *
     * @return IV value
     */
    public byte[] getBytes() {
        return bytes.clone();
    }

    /**
     * Verifies an IV was specified, and it is of the required length.
     *
     * @throws C3rIllegalArgumentException If the data provided for IV is invalid
     */
    private void validate() {
        if (bytes == null) {
            throw new C3rIllegalArgumentException("An IV may not be null.");
        } else if (bytes.length != IV_BYTE_LENGTH) {
            throw new C3rIllegalArgumentException(
                    "An IV must be " + IV_BYTE_LENGTH + " bytes in length, but was " + bytes.length + " bytes.");
        }
    }

}
