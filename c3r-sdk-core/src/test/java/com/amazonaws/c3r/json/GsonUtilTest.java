// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.json;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.TableSchema;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.utils.FileUtil;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class GsonUtilTest {
    @Test
    public void duplicateJsonSourceHeadersToTargetHeadersTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(
                "{ \"headerRow\": true, \"columns\": [ { \"sourceHeader\":\"target\", \"type\":\"fingerprint\" }," +
                        "{ \"sourceHeader\":\"target\", \"type\":\"fingerprint\" }, " +
                        "{ \"sourceHeader\":\"target\", \"type\":\"fingerprint\" } ] }", TableSchema.class));
    }

    @Test
    public void manyJsonInferredTargetHeadersTest() {
        final TableSchema tableSchema = GsonUtil.fromJson(
                "{ \"headerRow\": true, \"columns\": [ { \"sourceHeader\":\"target 1\", \"type\":\"fingerprint\" }, " +
                        "{ \"sourceHeader\":\"target 2\", \"type\":\"fingerprint\" }, " +
                        "{ \"sourceHeader\":\"target 3\", \"type\":\"fingerprint\" } ] }", TableSchema.class);
        assertEquals(3, tableSchema.getColumns().size());
        for (int i = 0; i < 3; i++) {
            assertEquals(
                    new ColumnHeader(tableSchema.getColumns().get(i).getSourceHeader() + ColumnHeader.DEFAULT_FINGERPRINT_SUFFIX),
                    tableSchema.getColumns().get(i).getTargetHeader());
        }
    }

    @Test
    public void jsonMixOfInferredAndNonInferredTargetHeadersTest() {
        final TableSchema tableSchema = GsonUtil.fromJson(
                "{ \"headerRow\": true, \"columns\": [ { \"sourceHeader\":\"inferred\", \"type\":\"fingerprint\" }, " +
                "{ \"sourceHeader\":\"src header\", \"targetHeader\":\"tgt header\", \"type\":\"fingerprint\" } ] }", TableSchema.class);
        assertEquals(2, tableSchema.getColumns().size());
        assertEquals(
                new ColumnHeader(tableSchema.getColumns().get(0).getSourceHeader() + ColumnHeader.DEFAULT_FINGERPRINT_SUFFIX),
                tableSchema.getColumns().get(0).getTargetHeader());
        assertEquals(new ColumnHeader("src header"), tableSchema.getColumns().get(1).getSourceHeader());
        assertEquals(new ColumnHeader("tgt header"), tableSchema.getColumns().get(1).getTargetHeader());
    }

    @Test
    public void jsonColumnHeaderToManyValidTargetsTest() {
        final TableSchema tableSchema = GsonUtil.fromJson(
                "{ \"headerRow\": true, \"columns\": [ " +
                        "{ \"sourceHeader\":\"target\", \"targetHeader\":\"target 0\", \"type\":\"fingerprint\" }, " +
                        "{ \"sourceHeader\":\"target\", \"targetHeader\":\"target 1\", \"type\":\"fingerprint\" }, " +
                        "{ \"sourceHeader\":\"target\", \"targetHeader\":\"target 2\", \"type\":\"fingerprint\" } ] }", TableSchema.class);
        assertEquals(3, tableSchema.getColumns().size());
        final List<ColumnSchema> configSpec = tableSchema.getColumns();
        // Check to make sure there's only one unique source header
        final Set<ColumnHeader> srcs = configSpec.stream().map(ColumnSchema::getSourceHeader).collect(Collectors.toSet());
        assertEquals(1, srcs.size());
        // Check to make sure there's three unique target headers
        final Set<ColumnHeader> tgts = configSpec.stream().map(ColumnSchema::getTargetHeader).collect(Collectors.toSet());
        assertEquals(3, tgts.size());
    }

    @Test
    public void jsonManyColumnHeadersToSingleTargetHeaderInvalidTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> GsonUtil.fromJson(
                "{ \"headerRow\": true, \"columns\": [ { \"sourceHeader\":\"target 0\", \"targetHeader\":\"target\", " +
                        "\"type\":\"fingerprint\" }, { \"sourceHeader\":\"target 1\", \"targetHeader\":\"target\", " +
                        "\"type\":\"fingerprint\" }, { \"sourceHeader\":\"target\", \"targetHeader\":\"target 2\", " +
                        "\"type\":\"fingerprint\" } ] }", TableSchema.class));
    }

    @Test
    public void fromJsonTest() {
        final TableSchema cfg = GsonUtil.fromJson(FileUtil.readBytes("../samples/schema/config_sample.json"), TableSchema.class);
        assertEquals(11, cfg.getColumns().size());
    }

    @Test
    public void toJsonBadTest() {
        assertThrows(C3rIllegalArgumentException.class, () ->
                GsonUtil.toJson("oops wrong object", TableSchema.class));
    }
}
