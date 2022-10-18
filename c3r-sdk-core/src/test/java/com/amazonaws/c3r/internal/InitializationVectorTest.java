// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.internal;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class InitializationVectorTest {

    @Test
    public void deriveIvTest() {
        // Expected byte[] SHA-256 hash of column label + nonce
        final InitializationVector expectedIv = new InitializationVector(new byte[]{106, 125, 22, 25, -9, -53, 121, 41, 78, -102, 89, -5});
        final Nonce nonce = new Nonce("nonce01234567890nonce01234567890".getBytes(StandardCharsets.UTF_8));
        final InitializationVector actualIv = InitializationVector.deriveIv("label", nonce);
        assertEquals(InitializationVector.IV_BYTE_LENGTH, actualIv.getBytes().length);
        assertArrayEquals(expectedIv.getBytes(), actualIv.getBytes());
    }

    @Test
    public void deriveIvChangesWithNonceTest() {
        final InitializationVector iv1 = InitializationVector.deriveIv("label", Nonce.nextNonce());
        final InitializationVector iv2 = InitializationVector.deriveIv("label", Nonce.nextNonce());
        assertFalse(Arrays.equals(iv1.getBytes(), iv2.getBytes()));
    }

    @Test
    public void deriveIvChangesWithLabelNonceTest() {
        final Nonce nonce = Nonce.nextNonce();
        final InitializationVector iv1 = InitializationVector.deriveIv("label1", nonce);
        final InitializationVector iv2 = InitializationVector.deriveIv("label2", nonce);
        assertFalse(Arrays.equals(iv1.getBytes(), iv2.getBytes()));
    }

    @Test
    public void deriveIvNullNonceTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> InitializationVector.deriveIv("label", null));
    }

    @Test
    public void deriveIvNullLabelTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> InitializationVector.deriveIv(null, Nonce.nextNonce()));
    }

    @Test
    public void deriveIvEmptyLabelTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> InitializationVector.deriveIv("", Nonce.nextNonce()));
    }

    @Test
    public void validateNullIvTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> new InitializationVector(null));
    }

    @Test
    public void validateEmptyIvTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> new InitializationVector(new byte[0]));
    }

    @Test
    public void validateSmallIvTest() {
        final byte[] ivBytes = "small".getBytes(StandardCharsets.UTF_8);
        assertThrows(C3rIllegalArgumentException.class, () -> new InitializationVector(ivBytes));
    }

    @Test
    public void validateLargeIvTest() {
        final byte[] ivBytes = ("AnIvMayOnlyBe" + InitializationVector.IV_BYTE_LENGTH + "BytesLong").getBytes(StandardCharsets.UTF_8);
        assertThrows(C3rIllegalArgumentException.class, () -> new InitializationVector(ivBytes));
    }
}
