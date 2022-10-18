// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption.keys;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;

import javax.crypto.SecretKey;

/**
 * This class is a wrapper on SecretKeys, providing bare minimum validation on a SecretKey. The class is package private and may not be
 * instantiated. See child classes {@link DerivedRootEncryptionKey} and {@link DerivedEncryptionKey} for more info.
 */
abstract class Key implements SecretKey {
    /**
     * Symmetric key.
     */
    protected final SecretKey secretKey;

    /**
     * Initialize symmetric key and validate the value.
     *
     * @param secretKey Symmetric key
     */
    Key(final SecretKey secretKey) {
        this.secretKey = secretKey;
        validate();
    }

    /**
     * Make sure key is not {@code null}.
     *
     * @throws C3rIllegalArgumentException If key is {@code null}
     */
    public void validate() {
        if (secretKey == null) {
            throw new C3rIllegalArgumentException("The SecretKey must not be null.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getAlgorithm() {
        return secretKey.getAlgorithm();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getFormat() {
        return secretKey.getFormat();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getEncoded() {
        return secretKey.getEncoded();
    }
}
