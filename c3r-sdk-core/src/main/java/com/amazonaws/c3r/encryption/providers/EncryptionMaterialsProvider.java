// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption.providers;

import com.amazonaws.c3r.encryption.EncryptionContext;
import com.amazonaws.c3r.encryption.materials.DecryptionMaterials;
import com.amazonaws.c3r.encryption.materials.EncryptionMaterials;

/**
 * Interface for providing encryption materials. Implementations are free to use any strategy for
 * providing encryption materials, such as simply providing static material that doesn't change, or
 * more complicated implementations, such as integrating with existing key management systems.
 */
public interface EncryptionMaterialsProvider {
    /**
     * Retrieves encryption materials matching the specified description from some source.
     *
     * @param context Information to assist in selecting the proper return value. The implementation
     *                is free to determine the minimum necessary for successful processing
     * @return The encryption materials that match the description, or null if no matching encryption
     *         materials found
     */
    DecryptionMaterials getDecryptionMaterials(EncryptionContext context);

    /**
     * Returns EncryptionMaterials which the caller can use for encryption. Each implementation of
     * EncryptionMaterialsProvider can choose its own strategy for loading encryption material. For
     * example, an implementation might load encryption material from an existing key management
     * system, or load new encryption material when keys are rotated.
     *
     * @param context Information to assist in selecting the proper return value. The implementation
     *                is free to determine the minimum necessary for successful processing
     * @return EncryptionMaterials which the caller can use to encrypt or decrypt data
     */
    EncryptionMaterials getEncryptionMaterials(EncryptionContext context);

    /**
     * Forces this encryption materials provider to refresh its encryption material. For many
     * implementations of encryption materials provider, this may simply be a no-op, such as any
     * encryption materials provider implementation that vends static/non-changing encryption
     * material. For other implementations that vend different encryption material throughout their
     * lifetime, this method should force the encryption materials provider to refresh its encryption
     * material.
     */
    void refresh();
}
