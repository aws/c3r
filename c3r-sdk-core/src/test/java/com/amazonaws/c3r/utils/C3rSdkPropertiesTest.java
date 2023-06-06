// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.utils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class C3rSdkPropertiesTest {

    @Test
    public void apiNameVersionTest() {
        assertEquals(C3rSdkProperties.VERSION, C3rSdkProperties.API_NAME.version());
    }

    @Test
    public void apiNameNameTest() {
        assertEquals("c3r-sdk", C3rSdkProperties.API_NAME.name());
    }

    @Test
    public void externalAndInternalVersionsMatchTest() throws IOException {
        // Ensure the version in `version.txt` (used by the build system and any other tooling outside the code base)
        // and the version constant in our codebase match.
        final String version = Files.readString(Path.of("../version.txt"), StandardCharsets.UTF_8).trim();
        assertEquals(version, C3rSdkProperties.VERSION);
    }
}
