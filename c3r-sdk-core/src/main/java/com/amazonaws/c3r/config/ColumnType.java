// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.CleartextTransformer;
import com.amazonaws.c3r.FingerprintTransformer;
import com.amazonaws.c3r.SealedTransformer;

/**
 * Differentiators for how a column is represented.
 */
public enum ColumnType {
    /**
     * Encrypted, meant to be used in the SELECT clause of an SQL query.
     */
    SEALED("sealed", SealedTransformer.class),

    /**
     * HMACed, meant to be used in ON clauses of an SQL query.
     */
    FINGERPRINT("fingerprint", FingerprintTransformer.class),

    /**
     * Cleartext, can be used in any clause of an SQL query.
     */
    CLEARTEXT("cleartext", CleartextTransformer.class);

    /**
     * Associated transformer.
     */
    private final Class<?> transformerClass;

    /**
     * Formatted version of name.
     */
    private final String name;

    /**
     * Associates column type with a specific transformer.
     *
     * @param name  How the enum should be displayed when transformed to a string
     * @param clazz Name of transformer class
     * @see com.amazonaws.c3r.Transformer
     */
    ColumnType(final String name, final Class<?> clazz) {
        this.name = name;
        this.transformerClass = clazz;
    }

    /**
     * Get the type of transformer that should be used for the column type.
     *
     * @return Corresponding transformer
     */
    public Class<?> getTransformerType() {
        return transformerClass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return name;
    }
}