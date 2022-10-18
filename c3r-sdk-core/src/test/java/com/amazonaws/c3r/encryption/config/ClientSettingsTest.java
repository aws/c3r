// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.encryption.config;

import com.amazonaws.c3r.config.ClientSettings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientSettingsTest {
    @Test
    public void builderTest() {
        final ClientSettings settings = ClientSettings.builder()
                .allowDuplicates(true)
                .allowJoinsOnColumnsWithDifferentNames(true)
                .allowCleartext(true)
                .preserveNulls(true).build();
        assertTrue(settings.isAllowDuplicates());
        assertTrue(settings.isAllowJoinsOnColumnsWithDifferentNames());
        assertTrue(settings.isAllowCleartext());
        assertTrue(settings.isPreserveNulls());
    }

    @Test
    public void highAssuranceModeTest() {
        final ClientSettings settings = ClientSettings.highAssuranceMode();
        assertFalse(settings.isAllowDuplicates());
        assertFalse(settings.isAllowJoinsOnColumnsWithDifferentNames());
        assertFalse(settings.isAllowCleartext());
        assertFalse(settings.isPreserveNulls());
    }

    @Test
    public void lowAssuranceModeTest() {
        final ClientSettings settings = ClientSettings.lowAssuranceMode();
        assertTrue(settings.isAllowDuplicates());
        assertTrue(settings.isAllowJoinsOnColumnsWithDifferentNames());
        assertTrue(settings.isAllowCleartext());
        assertTrue(settings.isPreserveNulls());
    }
}
