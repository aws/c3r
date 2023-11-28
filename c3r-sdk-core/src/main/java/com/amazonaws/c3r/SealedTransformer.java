// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.data.ClientDataInfo;
import com.amazonaws.c3r.data.ClientDataType;
import com.amazonaws.c3r.encryption.EncryptionContext;
import com.amazonaws.c3r.encryption.Encryptor;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.AdditionalAuthenticatedData;
import com.amazonaws.c3r.internal.InitializationVector;
import com.amazonaws.c3r.internal.Nonce;
import com.amazonaws.c3r.internal.PadUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

/**
 * Performs the marshalling/unmarshalling of data that may be used for column transfers between party members in a clean room.
 * Ciphertext will be encrypted based on the Provider contained in the Encryptor.
 *
 * <p>
 * Ciphertext produced by this class is meant for transferring and thus can be decrypted by a consumer.
 */
public class SealedTransformer extends Transformer {
    /**
     * The version of the {@code SealedTransformer} for compatability support.
     */
    static final byte[] FORMAT_VERSION = "01:".getBytes(StandardCharsets.UTF_8);

    /**
     * Indicating what type of cryptographic transformation was applied to the data and how it should be handled during decryption.
     */
    static final byte[] ENCRYPTION_DESCRIPTOR = "enc:".getBytes(StandardCharsets.UTF_8);

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
     * Used to confirm origin and authenticity of data.
     */
    private static final AdditionalAuthenticatedData AAD = new AdditionalAuthenticatedData(DESCRIPTOR_PREFIX);

    /**
     * Handles encrypting and decrypting data given the cryptographic settings such as {@code AAD} and key.
     */
    private final Encryptor encryptor;

    /**
     * Cryptographic settings that will be used in this clean room.
     */
    private final ClientSettings clientSettings;

    /**
     * Create an instance of a {@code SealedTransformer}.
     *
     * @param encryptor A specific HMAC implementation that will handle encryption/decryption
     * @param clientSettings Cryptographic settings for the clean room
     */
    public SealedTransformer(final Encryptor encryptor, final ClientSettings clientSettings) {
        this.encryptor = encryptor;
        this.clientSettings = clientSettings;
    }

    /**
     * Marshals cleartext data into Base64 encoded ciphertext using AES-GCM. Marshalled data is in the format:
     * <ul>
     *     <li>FORMAT_VERSION + ENCRYPTION_DESCRIPTOR + NONCE + IV + CIPHERTEXT + AUTH_TAG</li>
     * </ul>
     * Where:
     * <ul>
     *     <li>FORMAT_VERSION = 2 bytes in hexadecimal followed by a ":", representing the version of the SealedTransformer used for
     *         marshalling.</li>
     *     <li>ENCRYPTION_DESCRIPTOR = 4 bytes of "enc:" for marking the column as encrypted.</li>
     *     <li>NONCE = Provided via the EncryptionContext.</li>
     *     <li>IV = Generated with the Nonce and the column label stored in the EncryptionContext.</li>
     *     <li>CIPHERTEXT = Generated with the cleartext + padding (if any) + 2 byte pad size.</li>
     *     <li>AUTH_TAG = 16 byte AES-GCM tag.</li>
     * </ul>
     *
     * @param cleartext         The data to be encrypted, or null
     * @param encryptionContext The EncryptionContext for the data to be encrypted
     * @return Base64 encoded ciphertext with prefixed encryption data,
     *         or null if {@code cleartext == null} and {@code ClientSettings.preserveNull() == true}.
     * @throws C3rIllegalArgumentException If {@code EncryptionContext} is missing or data type is not supported
     */
    @Override
    public byte[] marshal(final byte[] cleartext, final EncryptionContext encryptionContext) {
        if (encryptionContext == null) {
            throw new C3rIllegalArgumentException("An EncryptionContext must be provided when encrypting.");
        }
        if (encryptionContext.getClientDataType() == null) {
            throw new C3rIllegalArgumentException("EncryptionContext missing ClientDataType when encrypting data for column `"
                    + encryptionContext.getColumnLabel() + "`.");
        }
        if (encryptionContext.getClientDataType() != ClientDataType.STRING) {
            throw new C3rIllegalArgumentException("Only string columns can be encrypted, but encountered non-string column `"
                    + encryptionContext.getColumnLabel() + "`.");
        }

        if (cleartext == null && clientSettings.isPreserveNulls()) {
            return null;
        }
        final var valueInfo = ClientDataInfo.builder()
                .type(encryptionContext.getClientDataType())
                .isNull(cleartext == null)
                .build();

        final byte[] paddedMessage = PadUtil.padMessage(cleartext, encryptionContext);
        final InitializationVector iv = InitializationVector.deriveIv(encryptionContext.getColumnLabel(), encryptionContext.getNonce());
        final byte[] fullPayload = ByteBuffer.allocate(ClientDataInfo.BYTE_LENGTH + paddedMessage.length)
                .put(valueInfo.encode())
                .put(paddedMessage).array();
        final byte[] ciphertext = encryptor.encrypt(fullPayload, iv, AAD, encryptionContext);
        final byte[] base64EncodedCiphertext = buildBase64EncodedMessage(ciphertext, encryptionContext.getNonce(), iv);
        final ByteBuffer marshalledMessage = ByteBuffer.allocate(DESCRIPTOR_PREFIX.length + base64EncodedCiphertext.length)
                .put(DESCRIPTOR_PREFIX)
                .put(base64EncodedCiphertext);
        final byte[] marshalledBytes = marshalledMessage.array();
        validateMarshalledByteLength(marshalledBytes);
        return marshalledBytes;
    }

    /**
     * Unmarshalls Base64 encoded ciphertext data into cleartext.
     *
     * @param content The Base64 encoded ciphertext with corresponding prefixed encryption data to be decrypted
     * @return The decoded cleartext
     * @throws C3rIllegalArgumentException If data type is not a string
     * @throws C3rRuntimeException         If the ciphertext couldn't be decoded from Base64
     */
    @Override
    public byte[] unmarshal(final byte[] content) {
        // Nulls must have been permitted when encrypting.
        if (content == null) {
            return null;
        }

        ByteBuffer marshalledCiphertext = ByteBuffer.wrap(content);
        // Verify format version
        verifyFormatVersion(marshalledCiphertext);

        // Verify descriptor
        verifyEncryptionDescriptor(marshalledCiphertext);

        // Decode Base64 Ciphertext String appearing after the descriptor prefix
        final byte[] base64EncodedCiphertext = new byte[marshalledCiphertext.remaining()];
        marshalledCiphertext.get(base64EncodedCiphertext);
        // Decode Base64 Ciphertext String
        try {
            marshalledCiphertext = ByteBuffer.wrap(Base64.getDecoder().decode(base64EncodedCiphertext));
        } catch (Exception e) {
            throw new C3rRuntimeException("Ciphertext could not be decoded from Base64.", e);
        }

        // Extract nonce.
        final Nonce nonce = extractNonce(marshalledCiphertext);

        // Extract IV
        final InitializationVector iv = extractIv(marshalledCiphertext);

        // Extract padded ciphertext data
        final byte[] ciphertext = extractCiphertext(marshalledCiphertext);

        final EncryptionContext encryptionContext = EncryptionContext.builder()
                .nonce(nonce)
                .columnLabel("UNMARSHAL") // unused during decryption.
                .build();

        // Decipher ciphertext
        final byte[] payload = encryptor.decrypt(ciphertext, iv, AAD, encryptionContext);

        final ClientDataInfo clientDataInfo = ClientDataInfo.decode(payload[0]);
        if (clientDataInfo.getType() != ClientDataType.STRING) {
            throw new C3rIllegalArgumentException("Expected encrypted data to be of type string, but found unsupported data type: "
                    + clientDataInfo.getType());
        }
        if (clientDataInfo.isNull()) {
            return null;
        }

        final byte[] paddedCleartext = Arrays.copyOfRange(payload, ClientDataInfo.BYTE_LENGTH, payload.length);
        // Remove padding
        return PadUtil.removePadding(paddedCleartext);
    }

    @Override
    public byte[] getVersion() {
        return FORMAT_VERSION.clone();
    }

    @Override
    byte[] getEncryptionDescriptor() {
        return ENCRYPTION_DESCRIPTOR.clone();
    }

    /**
     * Concatenates the nonce, IV, and ciphertext and then returns them as the Base64 encoded representation.
     *
     * @param ciphertext The ciphertext to add to the message
     * @param nonce      The nonce to add to the message
     * @param iv         The IV to add to the message
     * @return The base64 encoded message
     */
    byte[] buildBase64EncodedMessage(final byte[] ciphertext, final Nonce nonce, final InitializationVector iv) {
        final byte[] nonceBytes = nonce.getBytes();
        final byte[] ivBytes = iv.getBytes();
        final byte[] prefixedCiphertext = ByteBuffer.allocate(nonceBytes.length + ivBytes.length + ciphertext.length)
                .put(nonceBytes)
                .put(ivBytes)
                .put(ciphertext)
                .array();
        return Base64.getEncoder().encode(prefixedCiphertext);
    }

    /**
     * Ensure that the version information in the message matches the current version.
     * If there's a version mismatch, decryption may produce unexpected results.
     *
     * @param ciphertext The original marshalled ciphertext with all content
     * @throws C3rRuntimeException If the ciphertext is too short to extract version info from or value was invalid
     */
    void verifyFormatVersion(final ByteBuffer ciphertext) {
        if (ciphertext.remaining() < FORMAT_VERSION.length) {
            throw new C3rRuntimeException("Ciphertext missing version header, unable to decrypt.");
        }
        // Verify format version
        final byte[] versionNumber = new byte[FORMAT_VERSION.length];
        ciphertext.get(versionNumber);
        if (!Arrays.equals(FORMAT_VERSION, versionNumber)) {
            throw new C3rRuntimeException("Ciphertext version mismatch. Expected `" + Arrays.toString(FORMAT_VERSION)
                    + "` but was `" + Arrays.toString(versionNumber) + "`.");
        }
    }

    /**
     * Verifies that the encryption descriptor is part of the marshalled ciphertext. If not, this may not actually be ciphertext and
     * decryption may produce unexpected results.
     *
     * @param ciphertext The marshalled ciphertext with the FORMAT_VERSION removed from the front
     * @throws C3rRuntimeException If the ciphertext is too short to have the descriptor or the descriptor does not match sealed
     */
    void verifyEncryptionDescriptor(final ByteBuffer ciphertext) {
        if (ciphertext.remaining() < SealedTransformer.ENCRYPTION_DESCRIPTOR.length) {
            throw new C3rRuntimeException("Ciphertext missing description header, unable to decrypt.");
        }
        final byte[] encryptionDescriptor = new byte[ENCRYPTION_DESCRIPTOR.length];
        ciphertext.get(encryptionDescriptor);
        if (!Arrays.equals(ENCRYPTION_DESCRIPTOR, encryptionDescriptor)) {
            throw new C3rRuntimeException("Ciphertext descriptor mismatch. Expected `"
                    + new String(ENCRYPTION_DESCRIPTOR, StandardCharsets.UTF_8)
                    + "` but was `" + new String(encryptionDescriptor, StandardCharsets.UTF_8) + "`.");
        }
    }

    /**
     * Extracts the nonce from the marshalled base64 decoded ciphertext body.
     *
     * @param ciphertextBody The body of the ciphertext, post base64 decoding
     * @return The nonce from the head of the ciphertext body
     * @throws C3rRuntimeException If the ciphertext doesn't contain enough bytes for the nonce
     */
    Nonce extractNonce(final ByteBuffer ciphertextBody) {
        if (ciphertextBody.remaining() < Nonce.NONCE_BYTE_LENGTH) {
            throw new C3rRuntimeException("Ciphertext missing nonce, unable to decrypt.");
        }
        final byte[] nonceBytes = new byte[Nonce.NONCE_BYTE_LENGTH];
        ciphertextBody.get(nonceBytes);
        return new Nonce(nonceBytes);
    }

    /**
     * Extracts the IV from the marshalled base64 decoded ciphertext body.
     *
     * @param ciphertextBody The body of the ciphertext, post base64 decoding with
     *                       the nonce already removed from the head
     * @return The IV that was used for creating the ciphertext
     * @throws C3rRuntimeException If the ciphertext is too short to have an initialization vector
     */
    InitializationVector extractIv(final ByteBuffer ciphertextBody) {
        if (ciphertextBody.remaining() < InitializationVector.IV_BYTE_LENGTH) {
            throw new C3rRuntimeException("Ciphertext missing IV, unable to decrypt.");
        }
        final byte[] ivBytes = new byte[InitializationVector.IV_BYTE_LENGTH];
        ciphertextBody.get(ivBytes);
        return new InitializationVector(ivBytes);
    }

    /**
     * Extracts the ciphertext from the marshalled ciphertext.
     *
     * @param ciphertext The marshalled ciphertext
     * @return The ciphertext
     */
    byte[] extractCiphertext(final ByteBuffer ciphertext) {
        final byte[] paddedCiphertext = new byte[ciphertext.remaining()];
        ciphertext.get(paddedCiphertext);
        return paddedCiphertext;
    }
}
