// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.utils.FileTestUtility;
import com.amazonaws.c3r.utils.FileUtil;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static com.amazonaws.c3r.config.Config.getDefaultTargetFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConfigTest {

    @Test
    public void defaultTargetFileWithExtensionTest() {
        assertEquals("input.out.csv", getDefaultTargetFile("input.csv"));
        assertEquals("input.misc.out.csv", getDefaultTargetFile("input.misc.csv"));
    }

    @Test
    public void getDefaultTargetFileTest() throws IOException {
        final Path sourceFile = FileTestUtility.resolve("sourceFile.csv");

        final String defaultTargetFile = Config.getDefaultTargetFile(sourceFile.toFile().getAbsolutePath());

        // assert sourceFile directory is stripped and targetFile is associated with the working directory.
        final File targetFile = new File(defaultTargetFile);
        assertNotNull(sourceFile.getParent());
        assertNull(targetFile.getParentFile());
        assertTrue(targetFile.getAbsolutePath().contains(FileUtil.CURRENT_DIR));
    }

    @Test
    public void defaultTargetFileWithoutExtensionTest() {
        assertEquals("input.out", getDefaultTargetFile("input"));
    }
}
