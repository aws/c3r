// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption.materials;

import com.amazonaws.c3r.encryption.keys.DerivedRootEncryptionKey;

/**
 * Interface that specifies functions for decryption key usage.
 */
public interface DecryptionMaterials extends CryptographicMaterials {
    /**
     * Get the root decryption key in use.
     *
     * @return Decryption key
     */
    DerivedRootEncryptionKey getRootDecryptionKey();
}