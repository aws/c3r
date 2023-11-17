// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.data.ClientDataType;
import com.amazonaws.c3r.data.CsvValue;
import com.amazonaws.c3r.encryption.EncryptionContext;
import com.amazonaws.c3r.encryption.Encryptor;
import com.amazonaws.c3r.encryption.keys.KeyUtil;
import com.amazonaws.c3r.encryption.providers.SymmetricStaticProvider;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.internal.InitializationVector;
import com.amazonaws.c3r.internal.Nonce;
import com.amazonaws.c3r.internal.PadUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

import static com.amazonaws.c3r.data.ClientDataType.INT_BYTE_SIZE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SealedTransformerTest {
    private final Nonce nonce = new Nonce("nonce01234567890nonce01234567890".getBytes(StandardCharsets.UTF_8));

    private final EncryptionContext context = EncryptionContext.builder()
            .columnLabel("label")
            .nonce(nonce)
            .clientDataType(CsvValue.CLIENT_DATA_TYPE)
            .padType(PadType.NONE)
            .build();

    private final InitializationVector iv = InitializationVector.deriveIv(context.getColumnLabel(), nonce);

    private final byte[] ciphertextContent = "fakeCiphertext".getBytes(StandardCharsets.UTF_8);

    private final byte[] exampleDecodedCiphertextBody =
            ByteBuffer.allocate(Nonce.NONCE_BYTE_LENGTH
                            + InitializationVector.IV_BYTE_LENGTH
                            + ciphertextContent.length)
                    .put(nonce.getBytes())
                    .put(iv.getBytes())
                    .put(ciphertextContent)
                    .array();

    private final byte[] exampleDecodedCiphertextBodyBase64 = Base64.getEncoder().encode(exampleDecodedCiphertextBody);

    private final byte[] exampleCiphertext =
            ByteBuffer.allocate(SealedTransformer.DESCRIPTOR_PREFIX.length
                            + exampleDecodedCiphertextBodyBase64.length)
                    .put(SealedTransformer.DESCRIPTOR_PREFIX)
                    .put(exampleDecodedCiphertextBodyBase64)
                    .array();

    private SealedTransformer sealedTransformer;

    private Encryptor encryptor;

    @BeforeEach
    public void setup() {
        final byte[] secret = "SomeFakeSecretKey".getBytes(StandardCharsets.UTF_8);
        final SecretKey secretKey = new SecretKeySpec(secret, 0, secret.length, KeyUtil.KEY_ALG);
        final byte[] salt = "saltybytes".getBytes(StandardCharsets.UTF_8);
        final SymmetricStaticProvider provider = new SymmetricStaticProvider(secretKey, salt);
        encryptor = Encryptor.getInstance(provider);
        sealedTransformer = new SealedTransformer(encryptor, ClientSettings.highAssuranceMode());
    }

    private byte[] getDescriptorPrefix(final byte[] bytes) {
        return Arrays.copyOfRange(bytes, 0, SealedTransformer.DESCRIPTOR_PREFIX.length);
    }

    @Test
    public void buildBase64EncodedCiphertextEmptyMessageTest() {
        // Expected byte[] is Base64 of  nonce + IV
        final byte[] expectedPrefix = "bm9uY2UwMTIzNDU2Nzg5MG5vbmNlMDEyMzQ1Njc4OTBqfRYZ98t5KU6aWfs=".getBytes(StandardCharsets.UTF_8);
        final InitializationVector iv = InitializationVector.deriveIv(context.getColumnLabel(), nonce);
        final byte[] emptyMessage = new byte[0];
        final byte[] actualPrefix = sealedTransformer.buildBase64EncodedMessage(emptyMessage, nonce, iv);
        assertArrayEquals(expectedPrefix, actualPrefix);
    }

    @Test
    public void buildBase64EncodedCiphertextTest() {
        // Expected byte[] is Base64 of nonce + IV + message
        final byte[] expectedPrefix = "bm9uY2UwMTIzNDU2Nzg5MG5vbmNlMDEyMzQ1Njc4OTBqfRYZ98t5KU6aWftzb21lIG1lc3NhZ2U="
                .getBytes(StandardCharsets.UTF_8);
        final InitializationVector iv = InitializationVector.deriveIv(context.getColumnLabel(), nonce);
        final byte[] message = "some message".getBytes(StandardCharsets.UTF_8);
        final byte[] actualPrefix = sealedTransformer.buildBase64EncodedMessage(message, nonce, iv);
        assertArrayEquals(expectedPrefix, actualPrefix);
    }

    @Test
    public void marshalTest() {
        final byte[] cleartext = "some cleartext data".getBytes(StandardCharsets.UTF_8);
        final byte[] encryptedText = sealedTransformer.marshal(cleartext, context);

        assertArrayEquals(
                SealedTransformer.DESCRIPTOR_PREFIX,
                getDescriptorPrefix(encryptedText));

        final byte[] expectedText =
                "01:enc:bm9uY2UwMTIzNDU2Nzg5MG5vbmNlMDEyMzQ1Njc4OTBqfRYZ98t5KU6aWfthhMkpZDNxYAOeL9RWSNX8J7ByuBScwbxT/7vpSoRnTVWu0kXmHA=="
                        .getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(expectedText, encryptedText);

        final byte[] unmarshalledText = sealedTransformer.unmarshal(encryptedText);
        assertArrayEquals(cleartext, unmarshalledText);
    }

    @Test
    public void marshalMaxCleartextLengthWithinMaxStringLengthTest() {
        final byte[] cleartext = new byte[PadUtil.MAX_PADDED_CLEARTEXT_BYTES - PadUtil.MAX_PAD_BYTES];

        final EncryptionContext context = EncryptionContext.builder()
                .columnLabel("label")
                .clientDataType(CsvValue.CLIENT_DATA_TYPE)
                .maxValueLength(cleartext.length)
                .padLength(PadUtil.MAX_PAD_BYTES)
                .padType(PadType.MAX)
                .nonce(nonce).build();

        final byte[] encryptedText = sealedTransformer.marshal(cleartext, context);

        assertArrayEquals(
                SealedTransformer.DESCRIPTOR_PREFIX,
                getDescriptorPrefix(encryptedText));

        assertTrue(encryptedText.length < Transformer.MAX_GLUE_STRING_BYTES);

        final byte[] unmarshalledText = sealedTransformer.unmarshal(encryptedText);
        assertArrayEquals(cleartext, unmarshalledText);
    }

    @Test
    public void marshalledValueTooLongTest() {
        // The necessary bytes is really less than this, but it gets the point across
        final byte[] cleartext = new byte[Transformer.MAX_GLUE_STRING_BYTES + 1];

        final EncryptionContext context = EncryptionContext.builder()
                .columnLabel("label")
                .clientDataType(CsvValue.CLIENT_DATA_TYPE)
                .padType(PadType.NONE) // PadType must be none to avoid other restrictions preventing the error
                .nonce(nonce).build();

        assertThrows(C3rRuntimeException.class, () -> sealedTransformer.marshal(cleartext, context));
    }

    @Test
    public void marshalNullDataPreserveNullsFalseTest() {
        final byte[] encryptedBytes = sealedTransformer.marshal(null, context);
        final byte[] expectedBytes = ("01:enc:bm9uY2UwMTIzNDU2Nzg5MG5vbmNlMDEyMzQ1Njc4OTBqfRYZ98t5KU6aWftg96a4yADdecnBVZYmd8IxXr30")
                .getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(expectedBytes, encryptedBytes);
    }

    @Test
    public void marshalNullDataPreserveNullsTrueTest() {
        final byte[] secret = "SomeFakeSecretKey".getBytes(StandardCharsets.UTF_8);
        final SecretKey secretKey = new SecretKeySpec(secret, 0, secret.length, KeyUtil.KEY_ALG);
        final byte[] salt = "saltybytes".getBytes(StandardCharsets.UTF_8);
        final SymmetricStaticProvider provider = new SymmetricStaticProvider(secretKey, salt);
        sealedTransformer = new SealedTransformer(Encryptor.getInstance(provider), ClientSettings.lowAssuranceMode());
        final byte[] encryptedText = sealedTransformer.marshal(null, context);
        assertNull(encryptedText);
    }

    @Test
    public void marshalEmptyDataTest() {
        final byte[] encryptedText = sealedTransformer.marshal("".getBytes(StandardCharsets.UTF_8), context);
        // Strictly prefix and encrypted padding
        final byte[] expectedText = "01:enc:bm9uY2UwMTIzNDU2Nzg5MG5vbmNlMDEyMzQ1Njc4OTBqfRYZ98t5KU6aWfth96bE3B6pNqPzS3k9XRM+joTw"
                .getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(expectedText, encryptedText);

        final byte[] unmarshalledText = sealedTransformer.unmarshal(encryptedText);
        assertArrayEquals("".getBytes(StandardCharsets.UTF_8), unmarshalledText);
    }

    @Test
    public void marshalNullNonceTest() {
        final byte[] cleartext = "some cleartext data".getBytes(StandardCharsets.UTF_8);
        final EncryptionContext context = EncryptionContext.builder()
                .clientDataType(CsvValue.CLIENT_DATA_TYPE)
                .columnLabel("label")
                .padType(PadType.NONE)
                .nonce(null).build();
        assertThrows(C3rIllegalArgumentException.class, () -> sealedTransformer.marshal(cleartext, context));
    }

    @Test
    public void marshalNullEncryptionContextTest() {
        final byte[] cleartext = "some cleartext data".getBytes(StandardCharsets.UTF_8);
        assertThrows(C3rIllegalArgumentException.class, () -> sealedTransformer.marshal(cleartext, null));
    }

    @Test
    public void unmarshalInvalidNonceTest() {
        final byte[] cleartext = "some cleartext data".getBytes(StandardCharsets.UTF_8);
        final byte[] encryptedBytes = sealedTransformer.marshal(cleartext, context);
        encryptedBytes[6] = (byte) (encryptedBytes[6] - 1); // Manipulate a nonce byte
        assertThrows(C3rRuntimeException.class, () -> sealedTransformer.unmarshal(encryptedBytes));
    }

    @Test
    public void unmarshalInvalidEncodingTest() {
        final byte[] cleartext = "some cleartext data".getBytes(StandardCharsets.UTF_8);
        final ByteBuffer encryptedText = ByteBuffer.wrap(sealedTransformer.marshal(cleartext, context));
        // Non-Base64 chars
        final byte[] prefix = new byte[sealedTransformer.getEncryptionDescriptor().length + sealedTransformer.getVersion().length];
        encryptedText.get(prefix);
        // Make the file no longer formatted.
        final byte[] encodedCiphertext = new byte[encryptedText.remaining()];
        encryptedText.get(encodedCiphertext);
        final byte[] nonBase64EncryptedText = Base64.getDecoder().decode(encodedCiphertext);
        final byte[] badCiphertext = ByteBuffer.allocate(prefix.length + nonBase64EncryptedText.length)
                .put(prefix)
                .put(nonBase64EncryptedText)
                .array();
        assertThrows(C3rRuntimeException.class, () -> sealedTransformer.unmarshal(badCiphertext));
    }

    @Test
    public void unmarshalPreservedNullTest() {
        assertNull(sealedTransformer.unmarshal(null));
    }

    @Test
    public void unmarshalNullTest() {
        final byte[] encryptedText = sealedTransformer.marshal(null, context);
        // Strictly prefix and encrypted padding
        final byte[] expectedText = "01:enc:bm9uY2UwMTIzNDU2Nzg5MG5vbmNlMDEyMzQ1Njc4OTBqfRYZ98t5KU6aWftg96a4yADdecnBVZYmd8IxXr30"
                .getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(expectedText, encryptedText);

        final byte[] unmarshalledText = sealedTransformer.unmarshal(encryptedText);
        assertArrayEquals(null, unmarshalledText);
    }

    @Test
    public void prefixMessageTest() {
        final byte[] message = "some cleartext data".getBytes(StandardCharsets.UTF_8);
        final InitializationVector iv = InitializationVector.deriveIv(context.getColumnLabel(), nonce);
        final byte[] prefixedMessage = sealedTransformer.buildBase64EncodedMessage(message, nonce, iv);
        final ByteBuffer decodedPrefixMessage = ByteBuffer.wrap(Base64.getDecoder().decode(prefixedMessage));
        final byte[] prefixNonce = new byte[nonce.getBytes().length];
        decodedPrefixMessage.get(prefixNonce);
        assertArrayEquals(nonce.getBytes(), prefixNonce);
        final byte[] prefixIv = new byte[iv.getBytes().length];
        decodedPrefixMessage.get(prefixIv);
        assertArrayEquals(iv.getBytes(), prefixIv);
        final byte[] prefixMessage = new byte[message.length];
        decodedPrefixMessage.get(prefixMessage);
        assertArrayEquals(message, prefixMessage);
    }

    @Test
    public void verifyFormatVersionTest() {
        assertDoesNotThrow(() -> sealedTransformer.verifyFormatVersion(ByteBuffer.wrap(exampleCiphertext)));
    }

    @Test
    public void verifyFormatVersionInvalidTest() {
        // way too short
        assertThrows(C3rRuntimeException.class, () -> sealedTransformer.verifyFormatVersion(
                ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8))));
        // one character too short
        assertThrows(C3rRuntimeException.class, () -> sealedTransformer.verifyFormatVersion(
                ByteBuffer.wrap(Arrays.copyOfRange(
                        exampleCiphertext,
                        0,
                        SealedTransformer.FORMAT_VERSION.length - 1))));
        // flip the bits in one byte in an otherwise valid array of bytes, make sure it fails
        final byte[] oneByteWrong = Arrays.copyOf(exampleCiphertext, exampleCiphertext.length);
        // works before a bit flip
        assertDoesNotThrow(() -> sealedTransformer.verifyFormatVersion(ByteBuffer.wrap(oneByteWrong)));
        oneByteWrong[SealedTransformer.FORMAT_VERSION.length - 1] =
                (byte) ~oneByteWrong[SealedTransformer.FORMAT_VERSION.length - 1];
        // fails after a bit flip
        assertThrows(C3rRuntimeException.class, () -> sealedTransformer.verifyFormatVersion(
                ByteBuffer.wrap(oneByteWrong)));
    }

    @Test
    public void verifyEncryptionDescriptorTest() {
        assertDoesNotThrow(() -> sealedTransformer.verifyEncryptionDescriptor(ByteBuffer.wrap(exampleCiphertext)
                .position(SealedTransformer.FORMAT_VERSION.length)));
    }

    @Test
    public void verifyEncryptionDescriptorInvalidTest() {
        // way too short
        assertThrows(C3rRuntimeException.class, () -> sealedTransformer.verifyEncryptionDescriptor(
                ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8))));
        // one byte too short
        assertThrows(C3rRuntimeException.class, () -> sealedTransformer.verifyEncryptionDescriptor(
                ByteBuffer.wrap(Arrays.copyOfRange(
                        exampleCiphertext,
                        SealedTransformer.FORMAT_VERSION.length,
                        SealedTransformer.DESCRIPTOR_PREFIX.length - 1))));
        // flip the bits in one byte in an otherwise valid array of bytes, make sure it fails
        final byte[] oneByteWrong = Arrays.copyOfRange(
                exampleCiphertext,
                SealedTransformer.FORMAT_VERSION.length,
                exampleCiphertext.length);
        // works before a bit flip
        assertDoesNotThrow(() -> sealedTransformer.verifyEncryptionDescriptor(ByteBuffer.wrap(oneByteWrong)));
        oneByteWrong[SealedTransformer.FORMAT_VERSION.length] =
                (byte) ~oneByteWrong[SealedTransformer.FORMAT_VERSION.length];
        // fails after a bit flip
        assertThrows(C3rRuntimeException.class, () -> sealedTransformer.verifyEncryptionDescriptor(
                ByteBuffer.wrap(oneByteWrong)));
    }

    @Test
    public void extractNonceTest() {
        final Nonce extractedNonce = sealedTransformer.extractNonce(ByteBuffer.wrap(exampleDecodedCiphertextBody));
        assertArrayEquals(nonce.getBytes(), extractedNonce.getBytes());
    }

    @Test
    public void extractNonceBadTest() {
        // way too short
        assertThrows(C3rRuntimeException.class, () -> sealedTransformer.extractNonce(
                ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8))
        ));
        // one byte too short
        assertThrows(C3rRuntimeException.class, () -> sealedTransformer.verifyEncryptionDescriptor(
                ByteBuffer.wrap(Arrays.copyOfRange(
                        exampleDecodedCiphertextBody,
                        0,
                        Nonce.NONCE_BYTE_LENGTH - 1))));
    }

    @Test
    public void extractIvTest() {
        final InitializationVector extractedIv = sealedTransformer.extractIv(ByteBuffer.wrap(exampleDecodedCiphertextBody)
                .position(Nonce.NONCE_BYTE_LENGTH));
        assertArrayEquals(iv.getBytes(), extractedIv.getBytes());
    }

    @Test
    public void extractIvBadTest() {
        // way too short
        assertThrows(C3rRuntimeException.class, () -> sealedTransformer.extractIv(
                ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8))
        ));
        // one byte too short
        assertThrows(C3rRuntimeException.class, () -> sealedTransformer.extractIv(
                ByteBuffer.wrap(
                        Arrays.copyOfRange(
                                exampleDecodedCiphertextBody,
                                Nonce.NONCE_BYTE_LENGTH,
                                Nonce.NONCE_BYTE_LENGTH + InitializationVector.IV_BYTE_LENGTH - 1))));
    }

    @Test
    public void extractCiphertextTest() {
        final byte[] extractedContent = sealedTransformer.extractCiphertext(
                ByteBuffer.wrap(exampleDecodedCiphertextBody)
                        .position(Nonce.NONCE_BYTE_LENGTH + InitializationVector.IV_BYTE_LENGTH));
        assertArrayEquals(ciphertextContent, extractedContent);
    }

    @Test
    public void getEncryptionDescriptorImmutableTest() {
        final byte[] descriptor = sealedTransformer.getEncryptionDescriptor();
        Arrays.fill(descriptor, (byte) 0);
        assertFalse(Arrays.equals(sealedTransformer.getEncryptionDescriptor(), descriptor));
    }

    @Test
    public void getVersionImmutableTest() {
        final byte[] version = sealedTransformer.getVersion();
        Arrays.fill(version, (byte) 0);
        assertFalse(Arrays.equals(sealedTransformer.getVersion(), version));
    }

    @Test
    public void descriptorStringTest() {
        assertTrue(Transformer.hasDescriptor(
                sealedTransformer,
                SealedTransformer.DESCRIPTOR_PREFIX_STRING.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void marshalMissingClientDataTypeTest() {
        // Currently only encrypting strings is supported
        final byte[] cleartext = ByteBuffer.allocate(INT_BYTE_SIZE).putInt(42).array();
        final EncryptionContext context = EncryptionContext.builder()
                .clientDataType(null)
                .columnLabel("label")
                .nonce(nonce)
                .build();
        assertThrows(C3rIllegalArgumentException.class, () -> sealedTransformer.marshal(cleartext, context));
    }

    @Test
    public void marshalNonStringTest() {
        // Currently only encrypting strings is supported
        final byte[] cleartext = ByteBuffer.allocate(INT_BYTE_SIZE).putInt(42).array();
        final EncryptionContext context = EncryptionContext.builder()
                .clientDataType(ClientDataType.UNKNOWN)
                .columnLabel("label")
                .nonce(nonce)
                .build();
        assertThrows(C3rIllegalArgumentException.class, () -> sealedTransformer.marshal(cleartext, context));
    }
}
