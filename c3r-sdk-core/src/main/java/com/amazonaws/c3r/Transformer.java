// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.DecryptConfig;
import com.amazonaws.c3r.config.EncryptConfig;
import com.amazonaws.c3r.encryption.EncryptionContext;
import com.amazonaws.c3r.encryption.Encryptor;
import com.amazonaws.c3r.encryption.providers.SymmetricStaticProvider;
import com.amazonaws.c3r.exception.C3rRuntimeException;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Performs the marshalling/unmarshalling of ciphertext that may be used in a clean room.
 */
public abstract class Transformer {
    /**
     * Max size of any String post padding. This limit is imposed by Glue.
     */
    public static final int MAX_GLUE_STRING_BYTES = 16383;

    /**
     * Determines whether a given byte[] of marshalled data the appropriate descriptor in the prefix section for the passed
     * Transformer.
     *
     * @param transformer The Transformer providing a descriptor.
     * @param value       A byte[] of marshalled data.
     * @return True if the descriptor for the passed Transformer was found, else false.
     */
    public static boolean hasDescriptor(final Transformer transformer, final byte[] value) {
        final byte[] descriptor = transformer.getEncryptionDescriptor();
        final byte[] version = transformer.getVersion();
        // If value is blank or less than the length of the descriptor plus the translator version, short circuit and return false
        if (value == null || value.length < version.length + descriptor.length) {
            return false;
        }
        // First characters are the version. Next characters are the descriptor followed by encoded data
        final byte[] foundDescriptor = Arrays.copyOfRange(value, version.length, version.length + descriptor.length);
        return Arrays.equals(descriptor, foundDescriptor);
    }

    /**
     * Encrypts given cleartext based on settings.
     *
     * @param cleartext Data to be encrypted, or null.
     * @param context   Encryption context.
     * @return Ciphertext encrypted version of {@code cleartext}, or null depending on {@code context}.
     */
    public abstract byte[] marshal(byte[] cleartext, EncryptionContext context);

    /**
     * Decrypts given plain text based on settings.
     *
     * @param ciphertext Data to be decrypted, or null.
     * @return Cleartext encrypted version of {@code ciphertext}, or null if given null.
     */
    public abstract byte[] unmarshal(byte[] ciphertext);

    /**
     * Each Transformer stores a corresponding version stored as a 2 byte hex representation with a `:` at the end.
     * These versions may be used to determine if a marshalled value was produced by a given Transformer version.
     *
     * @return the version of the Transformer.
     */
    public abstract byte[] getVersion();

    /**
     * Each Transformer stores a corresponding descriptor with a `:` at the end. These descriptors may be used to determine
     * if a marshalled value was produced by the corresponding type of Transformer.
     *
     * @return the descriptor used by the Transformer.
     */
    abstract byte[] getEncryptionDescriptor();

    /**
     * Confirms the ciphertext is not null and is not too long for the database.
     *
     * @param marshalledBytes Ciphertext
     * @throws C3rRuntimeException If the ciphertext was null or longer than the allowed length
     */
    void validateMarshalledByteLength(final byte[] marshalledBytes) {
        if (marshalledBytes != null && marshalledBytes.length > MAX_GLUE_STRING_BYTES) {
            throw new C3rRuntimeException("Marshalled bytes too long for Glue. Glue supports a maximum length of "
                    + MAX_GLUE_STRING_BYTES + " bytes but marshalled value was " + marshalledBytes.length + " bytes.");
        }
    }

    /**
     * Create cryptographic transforms available for use.
     *
     * @param secretKey                Clean room key used to generate sub-keys for HMAC and encryption
     * @param salt                     Salt that can be publicly known but adds to randomness of cryptographic operations
     * @param settings                 Clean room cryptographic settings
     * @param failOnFingerprintColumns Whether to throw an error if a Fingerprint column is seen in the data
     * @return Mapping of {@link ColumnType} to the appropriate {@link Transformer}
     */
    public static Map<ColumnType, Transformer> initTransformers(final SecretKey secretKey, final String salt, final ClientSettings settings,
                                                                final boolean failOnFingerprintColumns) {
        final Encryptor encryptor = Encryptor.getInstance(new SymmetricStaticProvider(secretKey,
                salt.getBytes(StandardCharsets.UTF_8)));
        final Map<ColumnType, Transformer> transformers = new LinkedHashMap<>();
        transformers.put(ColumnType.CLEARTEXT, new CleartextTransformer());
        transformers.put(ColumnType.FINGERPRINT, new FingerprintTransformer(
                secretKey,
                salt.getBytes(StandardCharsets.UTF_8),
                settings,
                failOnFingerprintColumns));
        transformers.put(ColumnType.SEALED, new SealedTransformer(encryptor, settings));
        return transformers;
    }

    /**
     * Create cryptographic transforms available for use.
     *
     * @param config The cryptographic settings to use to initialize the transformers
     * @return Mapping of {@link ColumnType} to the appropriate {@link Transformer}
     */
    public static Map<ColumnType, Transformer> initTransformers(final EncryptConfig config) {
        return initTransformers(config.getSecretKey(),
                config.getSalt(),
                config.getSettings(),
                false); // FailOnFingerprintColumns not relevant to encryption
    }

    /**
     * Create cryptographic transforms available for use.
     *
     * @param config The cryptographic settings to use to initialize the transformers
     * @return Mapping of {@link ColumnType} to the appropriate {@link Transformer}
     */
    public static Map<ColumnType, Transformer> initTransformers(final DecryptConfig config) {
        return initTransformers(config.getSecretKey(),
                config.getSalt(),
                null, // Settings not relevant to decryption
                config.isFailOnFingerprintColumns());
    }
}
