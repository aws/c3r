// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.exception;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class C3rRuntimeExceptionTest {
    private final Exception sensitiveException = new RuntimeException("sensitive message");

    @Test
    public void getMessageTest() {
        // Ensure just the given message is returned and never an underlying message
        assertEquals("Doh!", new C3rRuntimeException("Doh!").getMessage());
        assertEquals("Doh!",
                new C3rRuntimeException("Doh!", sensitiveException).getMessage());
    }

    @Test
    public void getCauseTest() {
        // verify the underlying cause exception is being stored and returned as expected
        assertEquals(sensitiveException, new C3rRuntimeException("doh!", sensitiveException).getCause());
    }
}
