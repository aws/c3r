// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption.providers;

import com.amazonaws.c3r.encryption.EncryptionContext;
import com.amazonaws.c3r.encryption.keys.DerivedRootEncryptionKey;
import com.amazonaws.c3r.encryption.materials.DecryptionMaterials;
import com.amazonaws.c3r.encryption.materials.EncryptionMaterials;
import com.amazonaws.c3r.encryption.materials.SymmetricRawMaterials;

import javax.crypto.SecretKey;

/**
 * This EncryptionMaterialsProvider may be used when a SecretKey/salt has been shared out of band or is otherwise provided
 * programmatically. Data encrypted with this EncryptionMaterialsProvider may be decrypted with the same symmetric key and salt provided at
 * construction.
 */
public class SymmetricStaticProvider implements EncryptionMaterialsProvider {
    /**
     * Implements a symmetric key cryptographic algorithm.
     */
    private final SymmetricRawMaterials materials;

    /**
     * Creates a handler for symmetric keys, using {@code encryptionKey} and {@code salt} to generate sub-keys as needed.
     *
     * @param encryptionKey the key materials for the root key
     * @param salt          the salt for the root key
     */
    public SymmetricStaticProvider(final SecretKey encryptionKey, final byte[] salt) {
        materials = new SymmetricRawMaterials(new DerivedRootEncryptionKey(encryptionKey, salt));
    }


    /**
     * Returns the {@code encryptionKey} provided to the constructor.
     */
    @Override
    public DecryptionMaterials getDecryptionMaterials(final EncryptionContext context) {
        return new SymmetricRawMaterials(materials.getRootEncryptionKey());
    }

    /**
     * Returns the {@code encryptionKey} provided to the constructor.
     */
    @Override
    public EncryptionMaterials getEncryptionMaterials(final EncryptionContext context) {
        return new SymmetricRawMaterials(materials.getRootEncryptionKey());
    }

    /**
     * Does nothing.
     */
    @Override
    public void refresh() {
        // Do nothing
    }
}