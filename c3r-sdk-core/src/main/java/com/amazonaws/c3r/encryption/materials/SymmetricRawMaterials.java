// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption.materials;

import com.amazonaws.c3r.encryption.keys.DerivedRootEncryptionKey;

/**
 * Stores a symmetric root key, managing the encryption and decryption keys derived from it.
 */
public class SymmetricRawMaterials extends AbstractRawMaterials {
    /**
     * Symmetric key used for cryptographic operations.
     */
    private final DerivedRootEncryptionKey cryptoKey;

    /**
     * Stores root symmetric encryption key for managing encryption/decryption operations.
     *
     * @param encryptionKey Symmetric key
     */
    public SymmetricRawMaterials(final DerivedRootEncryptionKey encryptionKey) {
        this.cryptoKey = encryptionKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DerivedRootEncryptionKey getRootEncryptionKey() {
        return cryptoKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DerivedRootEncryptionKey getRootDecryptionKey() {
        return cryptoKey;
    }
}