// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.internal;

import java.util.regex.Pattern;

/**
 * Contains limits required for cryptographic guarantees
 * and other global correctness properties in a single place
 * for auditing.
 */
public final class Limits {
    /**
     * Max number of columns allowed in an output encrypted table.
     */
    public static final int ENCRYPTED_OUTPUT_COLUMN_COUNT_MAX = 1600;

    /**
     * Max number of encrypted rows across all tables from all providers (2^41).
     */
    public static final long ROW_COUNT_MAX = 2199023255552L;

    /**
     * Limit on header length (restricted by Glue).
     *
     * @deprecated This constant is no longer used - see {@link Limits#AWS_CLEAN_ROOMS_HEADER_MAX_LENGTH} instead.
     */
    @Deprecated
    public static final int GLUE_MAX_HEADER_UTF8_BYTE_LENGTH = 255;

    /**
     * Valid characters used for headers in Glue.
     *
     * @deprecated This constant is no longer used - see {@link Limits#AWS_CLEAN_ROOMS_HEADER_REGEXP} instead.
     */
    // Checkstyle doesn't like the escape characters in this string, but it is verbatim from the GLUE docs
    // and so it seems valuable to keep it as-is, so it's a 1-to-1 match.
    // CHECKSTYLE:OFF
    public static final String GLUE_VALID_HEADER_REGEXP = "[\u0020-\uD7FF\uE000-\uFFFD\uD800\uDC00-\uDBFF\uDFFF\t]*";
    // CHECKSTYLE:ON

    /**
     * Limit on header length (restricted by AWS Clean Rooms).
     *
     * @see <a href="https://docs.aws.amazon.com/clean-rooms/latest/apireference/API_Column.html">AWS Clean Rooms API Reference</a>
     */
    public static final int AWS_CLEAN_ROOMS_HEADER_MAX_LENGTH = 128;

    /**
     * Valid pattern for headers an AWS Clean Rooms header.
     *
     * @see <a href="https://docs.aws.amazon.com/clean-rooms/latest/apireference/API_Column.html">AWS Clean Rooms API Reference</a>
     */
    public static final Pattern AWS_CLEAN_ROOMS_HEADER_REGEXP =
            Pattern.compile("[a-z0-9_](([a-z0-9_ ]+-)*([a-z0-9_ ]+))?");

    /**
     * Hidden utility class constructor.
     */
    private Limits() {
    }

}