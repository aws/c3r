// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.internal;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;

import java.security.SecureRandom;

/**
 * Random number used for various cryptographic operations.
 */
public class Nonce {
    /**
     * Length of nonce in bytes.
     */
    public static final int NONCE_BYTE_LENGTH = 32;

    /**
     * Use the most secure CSPRNG available on the system to generate random numbers.
     */
    private static final SecureRandom NONCE_GENERATOR = new SecureRandom();

    /**
     * Random number.
     */
    private byte[] bytes;

    /**
     * Store a copy of an already existing nonce.
     *
     * @param bytes Random value
     */
    public Nonce(final byte[] bytes) {
        if (bytes != null) {
            this.bytes = bytes.clone();
        }

        validate();
    }

    /**
     * Generates a cryptographically secure nonce of {@value Nonce#NONCE_BYTE_LENGTH} bytes.
     *
     * @return A nonce consisting of random bytes
     */
    public static Nonce nextNonce() {
        final byte[] nonceBytes = new byte[Nonce.NONCE_BYTE_LENGTH];
        NONCE_GENERATOR.nextBytes(nonceBytes);
        return new Nonce(nonceBytes);
    }

    /**
     * Get a copy of the value.
     *
     * @return Unique copy of nonce
     */
    public byte[] getBytes() {
        return bytes.clone();
    }

    /**
     * Validates that the nonce is not {@code null} and is of the required length {@value #NONCE_BYTE_LENGTH} bytes.
     *
     * @throws C3rIllegalArgumentException If the nonce does not meet requirements
     */
    private void validate() {
        if (bytes == null) {
            throw new C3rIllegalArgumentException("A Nonce may not be null.");
        } else if (bytes.length != NONCE_BYTE_LENGTH) {
            throw new C3rIllegalArgumentException("An Nonce must be " + NONCE_BYTE_LENGTH + " bytes in length, but was " + bytes.length
                    + " bytes.");

        }
    }
}
