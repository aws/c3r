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
     * Max number of columns allowed in a table (2^30).
     *
     * <p>
     * Note: The cryptographic requirements actually limit
     * the number of encrypted rows to 2^32 and the number
     * of transfer columns to 2^30, but 2^30 on total
     * column count both implies those constraints and
     * is still almost certainly far more than any table
     * will contain anyway.
     *
     * <p>
     * NOTE: This constraint combined with the limit on the byte size of
     * output data imply the (infeasible to check) constraint that a single
     * row must contain less than 2^52 bytes of cleartext.
     */
    // Checkstyle treats the 30 as a magic number, but it doesn't make sense in this context to make it a separate variable.
    // CHECKSTYLE:OFF
    public static final int COLUMN_COUNT_MAX = 1 << 30;
    // CHECKSTYLE:ON

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