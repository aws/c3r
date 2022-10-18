// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption;

import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.internal.Nonce;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class EncryptionContextTest {
    private final Nonce nonce = new Nonce("nonce01234567890nonce01234567890".getBytes(StandardCharsets.UTF_8));

    @Test
    public void constructorTest() {
        final String label = "label";
        final int padLength = 1;
        final int maxValueLength = 5;
        final EncryptionContext context = EncryptionContext.builder()
                .padLength(padLength)
                .columnLabel(label)
                .padType(PadType.FIXED)
                .nonce(nonce)
                .maxValueLength(maxValueLength)
                .build();
        assertEquals(label, context.getColumnLabel());
        assertEquals(padLength, context.getPadLength());
        assertEquals(maxValueLength, context.getMaxValueLength());
        assertEquals(PadType.FIXED, context.getPadType());
        assertArrayEquals(nonce.getBytes(), context.getNonce().getBytes());
    }

    @Test
    public void constructorNullLabelTest() {
        final int padLength = 1;
        assertThrows(C3rIllegalArgumentException.class, () -> EncryptionContext.builder()
                .padLength(padLength)
                .columnLabel(null)
                .nonce(nonce)
                .build());
    }

    @Test
    public void constructorEmptyLabelTest() {
        final int padLength = 1;
        assertThrows(C3rIllegalArgumentException.class, () -> EncryptionContext.builder()
                .padLength(padLength)
                .columnLabel("")
                .nonce(nonce)
                .build());
    }
}
