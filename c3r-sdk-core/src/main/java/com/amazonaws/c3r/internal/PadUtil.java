// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.internal;

import com.amazonaws.c3r.encryption.EncryptionContext;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import lombok.NonNull;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Utility class for managing message padding.
 */
public abstract class PadUtil {
    /**
     * Max size of any cleartext bytes, post-padding, if being encrypted.
     */
    public static final int MAX_PADDED_CLEARTEXT_BYTES = 12000;

    /**
     * Max pad length.
     */
    public static final int MAX_PAD_BYTES = 10000;

    /**
     * Number of bytes used to encode the size of the padding, even if there is no padding.
     */
    static final int PAD_LENGTH_BYTES = Short.BYTES;

    /**
     * Uses the EncryptionContext to append a pad to a message
     * between 0 and {@value PadUtil#MAX_PAD_BYTES} bytes long.
     *
     * <p>
     * The final padded message may not be longer than {@value PadUtil#MAX_PADDED_CLEARTEXT_BYTES}
     *
     * <p>
     * The final padded message will always have {@value PadUtil#PAD_LENGTH_BYTES} appended to it in order to store the length of the pad.
     * These extra bytes do not count towards the {@value PadUtil#MAX_PADDED_CLEARTEXT_BYTES}.
     *
     * <p>
     * Padding types:
     * <ul>
     *     <li>MAX - pads message to the total length of {@link EncryptionContext#getMaxValueLength} plus
     *         {@link EncryptionContext#getPadLength}</li>
     *     <li>FIXED - pads message to a specified length of {@link EncryptionContext#getPadLength}</li>
     *     <li>NONE - do not append any padding</li>
     * </ul>
     *
     * @param encryptionContext The EncryptionContext for the column
     * @param message           The message to be padded
     * @return The message padded with a random byte sequence followed by a byte containing the padding size
     * @throws C3rIllegalArgumentException If the padding could not fit within the contextual length limit for this column and value
     */
    public static byte[] padMessage(@NonNull final byte[] message, final EncryptionContext encryptionContext) {
        if (encryptionContext == null) {
            throw new C3rIllegalArgumentException("An EncryptionContext must be provided when padding.");
        }
        final int paddingLength;
        switch (encryptionContext.getPadType()) {
            // MAX and FIXED use the same logic here, as the EncryptionContext
            // reasons differently in `getTargetPaddedLength` on how much to pad.
            case MAX:
            case FIXED:
                paddingLength = encryptionContext.getTargetPaddedLength() - message.length;
                final String baseError = "Error padding values for target column `" + encryptionContext.getColumnLabel() + "`:";
                if (paddingLength < 0) {
                    // The message to be padded doesn't have the room to be padded to the fixed length
                    throw new C3rIllegalArgumentException(
                            baseError + " No room for padding! Target padding length is "
                                    + encryptionContext.getTargetPaddedLength()
                                    + " bytes but message is already " + message.length + " bytes long.");
                }
                if (encryptionContext.getTargetPaddedLength() > MAX_PADDED_CLEARTEXT_BYTES) {
                    // The target message size exceeds the maximum
                    throw new C3rIllegalArgumentException(
                            baseError + " No room for padding! Target padding length is "
                                    + encryptionContext.getTargetPaddedLength()
                                    + " bytes but maximum padded size is " + (MAX_PADDED_CLEARTEXT_BYTES) + " bytes long.");
                }
                if (encryptionContext.getPadLength() < 0 || encryptionContext.getPadLength() > MAX_PAD_BYTES) {
                    // The target padding size exceeds the maximum
                    throw new C3rIllegalArgumentException(
                            baseError + " Padding length invalid! Padding length is "
                                    + encryptionContext.getPadLength()
                                    + " bytes but must be within the range of 0 to " + MAX_PAD_BYTES + " bytes long.");
                }
                break;
            case NONE:
            default:
                paddingLength = 0;
        }
        final byte[] pad = generatePad(paddingLength);
        return ByteBuffer.allocate(message.length + pad.length + PAD_LENGTH_BYTES)
                .put(message)
                .put(pad)
                .putShort((short) paddingLength)
                .array();
    }

    /**
     * Creates a pad of the specified length. Value can be constant as this will be encrypted.
     *
     * @param padLength The size of the pad to be created
     * @return The pad of random bytes of length padLength
     */
    static byte[] generatePad(final int padLength) {
        final byte[] padFill = new byte[padLength];
        Arrays.fill(padFill, (byte) 0x00);
        return padFill;
    }

    /**
     * Removes the pad from a padded message.
     *
     * @param paddedMessage The message with padding
     * @return The unpadded message
     */
    public static byte[] removePadding(final byte[] paddedMessage) {
        final ByteBuffer message = ByteBuffer.wrap(paddedMessage);
        // Last 2 bytes contain the length of the padding
        final int padLength = message.getShort(paddedMessage.length - PAD_LENGTH_BYTES);
        // Cleartext is the message up until the padding
        final byte[] cleartext = new byte[paddedMessage.length - PAD_LENGTH_BYTES - padLength];
        message.get(cleartext);
        return cleartext;
    }
}
