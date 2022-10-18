// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.exception;

/**
 * Like {@link IllegalArgumentException}, but contains an error message
 * that is always safe to be printed/logged.
 *
 * @see C3rRuntimeException
 */
public class C3rIllegalArgumentException extends C3rRuntimeException {
    /**
     * Construct an unchecked runtime exception for an invalid method parameter value.
     *
     * @param message Safe error message text for printing and logging
     */
    public C3rIllegalArgumentException(final String message) {
        super(message);
    }

    /**
     * Construct an unchecked runtime exception for an invalid method parameter value.
     *
     * @param message Safe error message text for printing and logging
     * @param cause Original error that may not be safe to print or log in case the user has a higher level of logging enabled
     */
    public C3rIllegalArgumentException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
