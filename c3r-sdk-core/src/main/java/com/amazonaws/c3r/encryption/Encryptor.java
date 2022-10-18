// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption;

import com.amazonaws.c3r.encryption.keys.DerivedEncryptionKey;
import com.amazonaws.c3r.encryption.keys.DerivedRootEncryptionKey;
import com.amazonaws.c3r.encryption.materials.DecryptionMaterials;
import com.amazonaws.c3r.encryption.materials.EncryptionMaterials;
import com.amazonaws.c3r.encryption.providers.EncryptionMaterialsProvider;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.AdditionalAuthenticatedData;
import com.amazonaws.c3r.internal.InitializationVector;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * This class handles the encryption and decryption of data using the CryptographicMaterialsProvider provided at instantiation.
 */
public final class Encryptor {
    /**
     * Configuration for encryption settings.
     */
    private static final String SYMMETRIC_ENCRYPTION_MODE = "AES/GCM/NoPadding";

    /**
     * MAC length.
     */
    private static final int AUTHENTICATION_TAG_LENGTH = 128;

    /**
     * Handles cryptographic operations.
     */
    private final EncryptionMaterialsProvider encryptionMaterialsProvider;

    /**
     * Create wrapper for cryptographic operations.
     *
     * @param provider Underlying cryptographic implementation
     */
    private Encryptor(final EncryptionMaterialsProvider provider) {
        this.encryptionMaterialsProvider = provider;
    }

    /**
     * Get an instance for cryptographic operations using the specified implementation.
     *
     * @param provider Underlying cryptographic implementation
     * @return Wrapper for cryptographic operations
     */
    public static Encryptor getInstance(final EncryptionMaterialsProvider provider) {
        return new Encryptor(provider);
    }

    /**
     * Encrypts the cleartext data into ciphertext.
     *
     * @param cleartext         The cleartext data to be encrypted.
     * @param iv                The IV to use for encryption.
     * @param aad               The AAD to use for encryption.
     * @param encryptionContext The EncryptionContext to use for encryption.
     * @return The encrypted cleartext data
     */
    public byte[] encrypt(final byte[] cleartext, final InitializationVector iv, final AdditionalAuthenticatedData aad,
                          final EncryptionContext encryptionContext) {
        final EncryptionMaterials encryptionMaterials = encryptionMaterialsProvider.getEncryptionMaterials(encryptionContext);
        final DerivedRootEncryptionKey key = encryptionMaterials.getRootEncryptionKey();
        return transform(cleartext, iv, aad, encryptionContext, key, Cipher.ENCRYPT_MODE);
    }

    /**
     * Decrypts the ciphertext data into cleartext.
     *
     * @param ciphertext        The ciphertext data to be decrypted
     * @param iv                The IV to use for decryption
     * @param aad               The AAD to use for decryption
     * @param encryptionContext The EncryptionContext to use for decryption
     * @return The decrypted cleartext data
     */
    public byte[] decrypt(final byte[] ciphertext, final InitializationVector iv, final AdditionalAuthenticatedData aad,
                          final EncryptionContext encryptionContext) {
        final DecryptionMaterials encryptionMaterials = encryptionMaterialsProvider.getDecryptionMaterials(encryptionContext);
        final DerivedRootEncryptionKey key = encryptionMaterials.getRootDecryptionKey();
        return transform(ciphertext, iv, aad, encryptionContext, key, Cipher.DECRYPT_MODE);
    }

    /**
     * Decrypt or encrypt data.
     *
     * @param message                  Data to operate on
     * @param iv                       Initialization vector
     * @param aad                      Additional authenticated data
     * @param encryptionContext        Cryptographic implementation to use
     * @param derivedRootEncryptionKey Key to use
     * @param mode                     Which cryptographic operation to perform
     * @return Results from cryptographic computation
     * @throws C3rIllegalArgumentException If the message, encryption information, block size or padding is incorrect.
     * @throws C3rRuntimeException         If the key could not initialize with specified settings
     */
    private byte[] transform(final byte[] message, final InitializationVector iv, final AdditionalAuthenticatedData aad,
                             final EncryptionContext encryptionContext, final DerivedRootEncryptionKey derivedRootEncryptionKey,
                             final int mode) {
        if (message == null) {
            throw new C3rIllegalArgumentException("Expected a message to transform but was given null.");
        }
        if (encryptionContext == null) {
            throw new C3rIllegalArgumentException("An EncryptionContext must always be provided when encrypting/decrypting, but was null.");
        }
        final DerivedEncryptionKey derivedEncryptionKey = new DerivedEncryptionKey(derivedRootEncryptionKey, encryptionContext.getNonce());

        final GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(AUTHENTICATION_TAG_LENGTH, iv.getBytes());
        final Cipher cipher;
        try {
            cipher = Cipher.getInstance(SYMMETRIC_ENCRYPTION_MODE);
            cipher.init(mode, derivedEncryptionKey, gcmParameterSpec);
        } catch (InvalidAlgorithmParameterException | InvalidKeyException e) {
            throw new C3rRuntimeException("Initialization for cipher `" + SYMMETRIC_ENCRYPTION_MODE + "` failed.", e);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
            throw new C3rRuntimeException("Requested cipher `" + SYMMETRIC_ENCRYPTION_MODE + "` is not available.", e);
        }
        if (aad != null) {
            cipher.updateAAD(aad.getBytes());
        }
        try {
            return cipher.doFinal(message);
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            final String error;
            if (mode == Cipher.ENCRYPT_MODE) {
                error = "Failed to encrypt data for target column `" + encryptionContext.getColumnLabel() + "`.";
            } else {
                error = "Failed to decrypt.";
            }
            throw new C3rRuntimeException(error, e);
        }
    }
}
