// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.internal;

import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.encryption.EncryptionContext;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PadUtilTest {
    private final Nonce nonce = new Nonce("nonce01234567890nonce01234567890".getBytes(StandardCharsets.UTF_8));

    @Test
    public void generatePadEmptyTest() {
        final byte[] pad = PadUtil.generatePad(0);
        assertArrayEquals(new byte[0], pad);
    }

    @Test
    public void generatePadTest() {
        final int padSize = 1000;
        final byte[] pad = PadUtil.generatePad(padSize);
        assertEquals(padSize, pad.length);
    }

    @Test
    public void padMessageNullEncryptionContextTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> PadUtil.padMessage(new byte[0], null));
    }

    @Test
    public void padMessagePadTypeFixedTest() {
        final byte[] message = "Some message to pad".getBytes(StandardCharsets.UTF_8);
        final EncryptionContext context = EncryptionContext.builder()
                .columnLabel("label")
                .nonce(nonce)
                .padType(PadType.FIXED)
                .padLength(100)
                .build();
        final byte[] paddedMessage = PadUtil.padMessage(message, context);
        assertEquals(100 + PadUtil.PAD_LENGTH_BYTES, paddedMessage.length);

        final byte[] unpaddedMessage = PadUtil.removePadding(paddedMessage);
        assertArrayEquals(message, unpaddedMessage);
    }

    @Test
    public void padMessagePadTypeFixedNotEnoughSpaceTest() {
        final byte[] message = "Some message to pad".getBytes(StandardCharsets.UTF_8);
        final EncryptionContext context = EncryptionContext.builder()
                .columnLabel("label")
                .nonce(nonce)
                .padType(PadType.FIXED)
                .padLength(1) // A value shorter than the message itself + PAD_LENGTH_SIZE
                .build();
        assertThrows(C3rIllegalArgumentException.class, () -> PadUtil.padMessage(message, context));
    }

    @Test
    public void padMessagePadTypeMaxTest() {
        final byte[] message = "Some message to pad".getBytes(StandardCharsets.UTF_8);
        final EncryptionContext context = EncryptionContext.builder()
                .columnLabel("label")
                .nonce(nonce)
                .padType(PadType.MAX)
                .padLength(100)
                .maxValueLength(50)
                .build();
        final byte[] paddedMessage = PadUtil.padMessage(message, context);
        assertEquals(150 + PadUtil.PAD_LENGTH_BYTES, paddedMessage.length);

        final byte[] unpaddedMessage = PadUtil.removePadding(paddedMessage);
        assertArrayEquals(message, unpaddedMessage);
    }

    @Test
    public void padMessagePadTypeMaxMaximumLengthTest() {
        final byte[] message = new byte[PadUtil.MAX_PADDED_CLEARTEXT_BYTES - PadUtil.MAX_PAD_BYTES];
        final EncryptionContext context = EncryptionContext.builder()
                .columnLabel("label")
                .nonce(nonce)
                .padType(PadType.MAX)
                .padLength(PadUtil.MAX_PAD_BYTES)
                .maxValueLength(message.length)
                .build();
        final byte[] paddedMessage = PadUtil.padMessage(message, context);
        assertEquals(PadUtil.MAX_PADDED_CLEARTEXT_BYTES + PadUtil.PAD_LENGTH_BYTES, paddedMessage.length);
    }

    @Test
    public void padMessagePadTypeMaxNotEnoughSpaceTest() {
        final byte[] message = "Some message to pad".getBytes(StandardCharsets.UTF_8);
        final EncryptionContext contextPadTooBig = EncryptionContext.builder()
                .columnLabel("label")
                .nonce(nonce)
                .padType(PadType.MAX)
                .padLength(PadUtil.MAX_PAD_BYTES)
                .maxValueLength(PadUtil.MAX_PADDED_CLEARTEXT_BYTES - PadUtil.MAX_PAD_BYTES + 1) // A value too big to pad
                .build();
        assertThrows(C3rIllegalArgumentException.class, () -> PadUtil.padMessage(message, contextPadTooBig));

        final EncryptionContext contextMaxValueTooBig = EncryptionContext.builder()
                .columnLabel("label")
                .nonce(nonce)
                .padType(PadType.MAX)
                .padLength(100)
                .maxValueLength(PadUtil.MAX_PADDED_CLEARTEXT_BYTES) // A value too big to pad
                .build();
        assertThrows(C3rIllegalArgumentException.class, () -> PadUtil.padMessage(new byte[PadUtil.MAX_PADDED_CLEARTEXT_BYTES],
                contextMaxValueTooBig));
    }

    @Test
    public void padMessagePadTypeFixedNotEnoughSpaceMaxStringTest() {
        final byte[] message = new byte[PadUtil.MAX_PADDED_CLEARTEXT_BYTES + 1];
        Arrays.fill(message, (byte) 0x00);
        final EncryptionContext context = EncryptionContext.builder()
                .columnLabel("label")
                .nonce(nonce)
                .padType(PadType.FIXED)
                .padLength(1)
                .build();
        assertThrows(C3rIllegalArgumentException.class, () -> PadUtil.padMessage(message, context));
    }

    @Test
    public void padMessagePadTooLargeTest() {
        final byte[] message = new byte[1];
        Arrays.fill(message, (byte) 0x00);
        final EncryptionContext context = EncryptionContext.builder()
                .columnLabel("label")
                .nonce(nonce)
                .padType(PadType.FIXED)
                .padLength(PadUtil.MAX_PAD_BYTES + 1) // The maximum size of a pad
                .build();
        assertThrows(C3rIllegalArgumentException.class, () -> PadUtil.padMessage(message, context));
    }

    @Test
    public void padMessagePadNegativeTest() {
        final byte[] message = new byte[1];
        Arrays.fill(message, (byte) 0x00);
        final EncryptionContext context = EncryptionContext.builder()
                .columnLabel("label")
                .nonce(nonce)
                .padType(PadType.FIXED)
                .padLength(-1) // The maximum size of a pad
                .build();
        assertThrows(C3rIllegalArgumentException.class, () -> PadUtil.padMessage(message, context));
    }

    @Test
    public void padMessagePadTypeNoneTest() {
        final byte[] message = "Some message to pad".getBytes(StandardCharsets.UTF_8);
        final EncryptionContext context = EncryptionContext.builder()
                .columnLabel("label")
                .nonce(nonce)
                .padType(PadType.NONE)
                .padLength(100) // to assert NONE is respected over length
                .build();
        final byte[] paddedMessage = PadUtil.padMessage(message, context);
        assertEquals(message.length, paddedMessage.length - PadUtil.PAD_LENGTH_BYTES);

        final byte[] unpaddedMessage = PadUtil.removePadding(paddedMessage);
        assertArrayEquals(message, unpaddedMessage);
    }
}
