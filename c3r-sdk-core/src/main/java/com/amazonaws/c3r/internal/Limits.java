// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.internal;

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
     * @see <a href="https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-tables.html">AWS Glue API Catalog Tables</a>
     */
    public static final int GLUE_MAX_HEADER_UTF8_BYTE_LENGTH = 255;

    /**
     * Valid characters used for headers in Glue.
     *
     * @see <a href="https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-common.html#aws-glue-api-regex-oneLine">
     *       AWS Glue API Regex One Line</a>
     */
    // Checkstyle doesn't like the escape characters in this string, but it is verbatim from the GLUE docs
    // and so it seems valuable to keep it as-is, so it's a 1-to-1 match.
    // CHECKSTYLE:OFF
    public static final String GLUE_VALID_HEADER_REGEXP = "[\u0020-\uD7FF\uE000-\uFFFD\uD800\uDC00-\uDBFF\uDFFF\t]*";
    // CHECKSTYLE:ON

    /**
     * Hidden utility class constructor.
     */
    private Limits() {
    }

}