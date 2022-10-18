// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.internal;

/**
 * Stores AAD for use during operation to confirm origin and authenticity of data.
 */
public class AdditionalAuthenticatedData {
    /**
     * AAD value.
     */
    private byte[] bytes;

    /**
     * Stores a value used to verify data is from the expected source.
     *
     * @param bytes Value to identify your data as authentic
     */
    public AdditionalAuthenticatedData(final byte[] bytes) {
        if (bytes != null) {
            this.bytes = bytes.clone();
        }
    }

    /**
     * Get the AAD value.
     *
     * @return AAD
     */
    public byte[] getBytes() {
        if (bytes != null) {
            return bytes.clone();
        }
        return null;
    }
}
