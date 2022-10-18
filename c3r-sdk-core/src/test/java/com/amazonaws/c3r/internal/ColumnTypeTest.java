// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.internal;

import com.amazonaws.c3r.CleartextTransformer;
import com.amazonaws.c3r.FingerprintTransformer;
import com.amazonaws.c3r.SealedTransformer;
import com.amazonaws.c3r.config.ColumnType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ColumnTypeTest {

    @Test
    public void cleartextColumnTypeTest() {
        assertEquals(CleartextTransformer.class, ColumnType.CLEARTEXT.getTransformerType());
    }

    @Test
    public void fingerprintColumnTypeTest() {
        assertEquals(FingerprintTransformer.class, ColumnType.FINGERPRINT.getTransformerType());
    }

    @Test
    public void sealedColumnTypeTest() {
        assertEquals(SealedTransformer.class, ColumnType.SEALED.getTransformerType());
    }
}
