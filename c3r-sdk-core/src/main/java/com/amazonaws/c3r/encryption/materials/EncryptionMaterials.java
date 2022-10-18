// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption.materials;

import com.amazonaws.c3r.encryption.keys.DerivedRootEncryptionKey;

/**
 * Interface that specifies functions for encryption key usage.
 */
public interface EncryptionMaterials extends CryptographicMaterials {
    /**
     * Get the root encryption key in use.
     *
     * @return Encryption key
     */
    DerivedRootEncryptionKey getRootEncryptionKey();
}