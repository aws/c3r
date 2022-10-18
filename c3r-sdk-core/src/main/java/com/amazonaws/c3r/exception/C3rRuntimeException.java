// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.exception;

/**
 * Like {@link RuntimeException}, but contains an error message
 * that is always safe to be printed/logged.
 */
public class C3rRuntimeException extends RuntimeException {
    /**
     * Construct an unchecked runtime exception.
     *
     * @param message Safe error message for printing and logging
     */
    public C3rRuntimeException(final String message) {
        super(message);
    }

    /**
     * Construct an unchecked runtime exception.
     *
     * @param message Safe error message for printing and logging
     * @param cause Original error that may not be safe to print or log in case the user has a higher level of logging enabled
     */
    public C3rRuntimeException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
