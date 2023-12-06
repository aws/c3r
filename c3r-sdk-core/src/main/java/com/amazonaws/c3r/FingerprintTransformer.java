// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.encryption.EncryptionContext;
import com.amazonaws.c3r.encryption.keys.KeyUtil;
import com.amazonaws.c3r.encryption.keys.SaltedHkdf;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import lombok.extern.slf4j.Slf4j;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

/**
 * Performs the marshalling/unmarshalling of data that may be used for column matching between party members in a clean room.
 *
 * <p>
 * By default, ciphertext will be produced with HmacSHA256. A developer may also choose to encrypt on a per-column or per-row basis.
 *
 * <p>
 * Ciphertext produced by this class is meant for matching and is NOT intended to be returned. It can not be unmarshalled.
 * Attempting to unmarshall match data in tests or otherwise will return the data AS IS.
 */
@Slf4j
public class FingerprintTransformer extends Transformer {
    /**
     * Application context stored inside the created mac key.
     *
     * @see KeyUtil#HKDF_INFO
     */
    static final byte[] HKDF_INFO_BYTES = KeyUtil.HKDF_INFO.getBytes(StandardCharsets.UTF_8);

    /**
     * Indicating what type of cryptographic transformation was applied to the data and how it should be handled during decryption.
     */
    private static final byte[] ENCRYPTION_DESCRIPTOR = "hmac:".getBytes(StandardCharsets.UTF_8);

    /**
     * The version of the {@code FingerprintTransformer} for compatability support.
     */
    private static final byte[] FORMAT_VERSION = "02:".getBytes(StandardCharsets.UTF_8);

    /**
     * Combined format version and encryption description that will be attached as a prefix to the ciphertext so the correct processing is
     * done on decryption as a String.
     */
    public static final String DESCRIPTOR_PREFIX_STRING =
            new String(FORMAT_VERSION, StandardCharsets.UTF_8) + new String(ENCRYPTION_DESCRIPTOR, StandardCharsets.UTF_8);

    /**
     * Combined format version and encryption description that will be attached as a prefix to the ciphertext so the correct processing is
     * done on decryption as bytes.
     */
    static final byte[] DESCRIPTOR_PREFIX = DESCRIPTOR_PREFIX_STRING.getBytes(StandardCharsets.UTF_8);

    /**
     * Number of bytes in the HMAC key.
     */
    private static final int HMAC_KEY_SIZE = 32;

    /**
     * HKDF created with mac and salt for this clean room.
     */
    private final SaltedHkdf hkdf;

    /**
     * Cryptographic settings for the clean room.
     */
    private final ClientSettings clientSettings;

    /**
     * Whether fingerprint columns should be permitted in decrypted data or not since they can't be reversed and provide no information.
     */
    private final boolean failOnUnmarshal;

    /**
     * Generates MACs based on {@link KeyUtil#HMAC_ALG} specified algorithm and key.
     */
    private final Mac mac;

    /**
     * Whether to warn user if a fingerprint column was included in decrypted output.
     */
    private boolean unmarshalWarningRaised = false;

    /**
     * Constructs a FingerprintTransformer that will use the HMAC algorithm "HmacSHA256".
     *
     * @param secretKey       The secretKey used to instantiate the HKDF
     * @param salt            The salt used to instantiate the HKDF
     * @param clientSettings  ClientSettings to use when determining things like whether to use the same secret key for every column or
     *                        use per column secrets
     * @param failOnUnmarshal Whether to throw an error if calling unmarshall on a Fingerprint column
     * @throws C3rRuntimeException If the FingerprintTransformer can't be initialized with given cryptographic specifications
     */
    public FingerprintTransformer(final SecretKey secretKey, final byte[] salt, final ClientSettings clientSettings,
                                  final boolean failOnUnmarshal) {
        this.clientSettings = clientSettings;
        this.failOnUnmarshal = failOnUnmarshal;
        try {
            hkdf = new SaltedHkdf(secretKey, salt);
            mac = Mac.getInstance(KeyUtil.HMAC_ALG);
        } catch (NoSuchAlgorithmException e) {
            throw new C3rRuntimeException("Could not initialize FingerprintTransformer.", e);
        }
    }

    /**
     * Marshals cleartext data into Base64 encoded ciphertext using HmacSHA256. Marshalled data is in the format:
     * <ul>
     *     <li>FORMAT_VERSION + ENCRYPTION_DESCRIPTOR + CIPHERTEXT</li>
     * </ul>
     * Where:
     * <ul>
     *     <li>FORMAT_VERSION = 2 bytes in hexadecimal followed by a ":", representing the version of the FingerprintTransformer used for
     *         marshalling.</li>
     *     <li>ENCRYPTION_DESCRIPTOR = 5 bytes of "hmac:" for marking the column as HMACed.</li>
     *     <li>CIPHERTEXT = Generated with the cleartext and the column label from the EncryptionContext if
     *         allowJoinsOnColumnsWithDifferentNames is false</li>
     * </ul>
     *
     * @param cleartext         The data to be HMACed
     * @param encryptionContext The EncryptionContext for the data to be HMACed
     * @return base64 encoded HMAC of the cleartext with prefix, or null if given null
     * @throws C3rIllegalArgumentException If encryption context is missing or data type is not a string
     * @throws C3rRuntimeException         If the HMAC algorithm couldn't be configured
     */
    @Override
    public byte[] marshal(final byte[] cleartext, final EncryptionContext encryptionContext) {
        if (encryptionContext == null) {
            throw new C3rIllegalArgumentException("An EncryptionContext must be provided when marshaling.");
        }
        if (encryptionContext.getClientDataType() == null) {
            throw new C3rIllegalArgumentException("EncryptionContext missing ClientDataType when encrypting data for column `"
                    + encryptionContext.getColumnLabel() + "`.");
        }
        if (!encryptionContext.getClientDataType().supportsFingerprintColumns()) {
            throw new C3rIllegalArgumentException(encryptionContext.getClientDataType() + " is not a type supported by " +
                    "fingerprint columns.");
        }
        if (!encryptionContext.getClientDataType().isEquivalenceClassRepresentativeType()) {
            throw new C3rIllegalArgumentException(encryptionContext.getClientDataType() + " is not the parent type of its equivalence " +
                    "class. Expected parent type is " + encryptionContext.getClientDataType().getRepresentativeType() + ".");
        }

        // Check if a plain null value should be used
        if (cleartext == null) {
            if (clientSettings.isPreserveNulls()) {
                return null;
            }
        }

        final byte[] key;
        if (clientSettings.isAllowJoinsOnColumnsWithDifferentNames()) {
            key = hkdf.deriveKey(HKDF_INFO_BYTES, HMAC_KEY_SIZE);
        } else {
            final byte[] hkdfKeyInfo = (KeyUtil.HKDF_COLUMN_BASED_INFO + encryptionContext.getColumnLabel())
                    .getBytes(StandardCharsets.UTF_8);
            key = hkdf.deriveKey(hkdfKeyInfo, HMAC_KEY_SIZE);
        }
        final SecretKeySpec secretKeySpec = new SecretKeySpec(key, mac.getAlgorithm());
        Arrays.fill(key, (byte) 0); // Safe to zero here. SecretKeySpec takes a clone on instantiation.
        try {
            mac.init(secretKeySpec);
        } catch (InvalidKeyException e) {
            throw new C3rRuntimeException("Initialization of hmac failed for target column `"
                    + encryptionContext.getColumnLabel() + "`.", e);
        }
        final byte[] hmacBase64 = Base64.getEncoder().encode(mac.doFinal(cleartext));
        final byte[] marshalledBytes = ByteBuffer.allocate(DESCRIPTOR_PREFIX.length + hmacBase64.length)
                .put(DESCRIPTOR_PREFIX)
                .put(hmacBase64)
                .array();
        validateMarshalledByteLength(marshalledBytes);
        return marshalledBytes;
    }

    /**
     * Logs a warning when unmarshalling HMACed data since it's a one-way transform unless {@link #failOnUnmarshal} is true then an error
     * is thrown.
     *
     * @param ciphertext The ciphertext of HMAC data or null.
     * @return {@code ciphertext} value
     * @throws C3rRuntimeException If {@link #failOnUnmarshal} set to true and fingerprint column encountered
     */
    @Override
    public byte[] unmarshal(final byte[] ciphertext) {
        if (failOnUnmarshal) {
            throw new C3rRuntimeException("Data encrypted for a fingerprint column was found but is forbidden with current settings.");
        }
        if (!unmarshalWarningRaised) {
            unmarshalWarningRaised = true;
            log.warn("Data encrypted for a fingerprint column was found. Encrypted fingerprint column data cannot be decrypted and " +
                    "will appear as-is in the output.");
        }
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
     * Gets the descriptor for the FingerprintTransformer.
     *
     * @return {@link #ENCRYPTION_DESCRIPTOR}
     */
    @Override
    byte[] getEncryptionDescriptor() {
        return ENCRYPTION_DESCRIPTOR.clone();
    }
}