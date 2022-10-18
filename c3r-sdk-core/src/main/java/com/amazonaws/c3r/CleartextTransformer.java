// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r;

import com.amazonaws.c3r.encryption.EncryptionContext;
import com.amazonaws.c3r.exception.C3rRuntimeException;

import java.nio.charset.StandardCharsets;

/**
 * Performs the marshalling/unmarshalling of data that may be used for cleartext columns between party members in a clean room.
 *
 * <p>
 * This Transformer performs a no-op transform. Marshalling or unmarshalling data will return the data AS IS.
 */
public class CleartextTransformer extends Transformer {
    /**
     * The version of the {@code CleartextTransformer} for compatability support.
     */
    private static final byte[] FORMAT_VERSION = "01:".getBytes(StandardCharsets.UTF_8);

    /**
     * Empty constructor since no context is needed.
     */
    public CleartextTransformer() {
    }

    /**
     * Validates the cleartext and returns it unchanged.
     *
     * @param cleartext Data to be passed through as cleartext, or null
     * @param context   Encryption context of cryptographic settings, not applied in this case
     * @return {@code cleartext} unmodified
     */
    @Override
    public byte[] marshal(final byte[] cleartext, final EncryptionContext context) {
        validateMarshalledByteLength(cleartext);
        return cleartext;
    }

    /**
     * Processes the cleartext data during decryption.
     *
     * @param ciphertext Cleartext being unmarshalled
     * @return {@code ciphertext} unmodified
     */
    @Override
    public byte[] unmarshal(final byte[] ciphertext) {
        return ciphertext;
    }

    /**
     * Gets the current format version.
     *
     * @return {@link #FORMAT_VERSION}
     */
    @Override
    public byte[] getVersion() {
        return FORMAT_VERSION.clone();
    }

    /**
     * There is no encryption descriptor for this type since no encryption is performed. If this function is called, an error is thrown.
     *
     * @return Nothing, only throws an exception
     * @throws C3rRuntimeException Operation is not supported for CleartextTransformer, exception always thrown
     */
    @Override
    byte[] getEncryptionDescriptor() {
        // No descriptor is used for cleartext data.
        throw new C3rRuntimeException("This operation is not supported for the CleartextTransformer.");
    }
}
