// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.internal;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NonceTest {

    @RepeatedTest(100)
    public void getNonceTest() {
        // Ensure nonces aren't equal
        final Nonce nonce1 = Nonce.nextNonce();
        final Nonce nonce2 = Nonce.nextNonce();
        assertFalse(Arrays.equals(nonce1.getBytes(), nonce2.getBytes()));
    }

    @Test
    public void validateNullNonceTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> new Nonce(null));
    }

    @Test
    public void validateEmptyNonceTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> new Nonce(new byte[0]));
    }

    @Test
    public void validateSmallNonceTest() {
        final byte[] nonceBytes = "small".getBytes(StandardCharsets.UTF_8);
        assertThrows(C3rIllegalArgumentException.class, () -> new Nonce(nonceBytes));
    }

    @Test
    public void validateLargeNonceTest() {
        final byte[] nonceBytes = ("ANonceMayOnlyBe" + Nonce.NONCE_BYTE_LENGTH + "BytesLong").getBytes(StandardCharsets.UTF_8);
        assertThrows(C3rIllegalArgumentException.class, () -> new Nonce(nonceBytes));
    }

}
