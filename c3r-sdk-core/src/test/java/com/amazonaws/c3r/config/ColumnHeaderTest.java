// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.internal.Limits;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ColumnHeaderTest {
    @Test
    public void checkNullValueToConstructorTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> new ColumnHeader(null));

    }

    @Test
    public void checkEmptyStringToConstructorTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> new ColumnHeader(""));

    }

    @Test
    public void checkBlankStringToConstructorTest() {
        assertThrows(C3rIllegalArgumentException.class, () -> new ColumnHeader("\n    \t "));
    }

    /*
     * Verify normalization of headers along with equality and hashing.
     * - White space is trimmed
     * - Header is all lowercase
     */
    @Test
    public void checkMixedCaseStringTest() {
        final String str = "\tThIs iS a WEIrD StrING \n";
        final String expected = "this is a weird string";
        final ColumnHeader ch = new ColumnHeader(str);
        final ColumnHeader chExpected = new ColumnHeader(expected);
        final ColumnHeader notMatch = new ColumnHeader("test");

        assertNotNull(ch);
        assertEquals(chExpected, ch);
        assertNotEquals(notMatch, ch);
        assertEquals(expected, ch.toString());
        assertNotEquals(notMatch.toString(), ch.toString());
        assertEquals(chExpected.hashCode(), ch.hashCode());
        assertNotEquals(notMatch.hashCode(), ch.hashCode());
    }

    @Test
    public void checkGlueMaxLengthStringConstructorTest() {
        assertDoesNotThrow(
                () -> new ColumnHeader("a".repeat(Limits.GLUE_MAX_HEADER_UTF8_BYTE_LENGTH)));
        assertThrows(
                C3rIllegalArgumentException.class,
                () -> new ColumnHeader("a".repeat(Limits.GLUE_MAX_HEADER_UTF8_BYTE_LENGTH + 1)));
    }

    @Test
    public void checkHeaderNonGlueConformantHeaderNameTest() {
        assertFalse(Limits.GLUE_VALID_HEADER_REGEXP.matches("Multi Line\n Header"));
        assertThrows(
                C3rIllegalArgumentException.class,
                () -> new ColumnHeader("Multi Line\n Header"));
    }

    // Check that we generate the expected valid and invalid results when creating a header from index.
    @Test
    public void columnHeaderFromIndexTest() {
        assertEquals(new ColumnHeader("Column 1"), ColumnHeader.getColumnHeaderFromIndex(0));
        assertThrows(C3rIllegalArgumentException.class, () -> ColumnHeader.getColumnHeaderFromIndex(-10));
    }

    // Make sure we check all cases of configuring potentially unnamed target headers and verify output.
    @Test
    public void deriveTargetColumnHeaderTest() {
        final ColumnHeader source = new ColumnHeader("source");
        final ColumnHeader target = new ColumnHeader("target");

        final ArrayList<ArrayList<ColumnHeader>> cases = new ArrayList<>() {
            {
                add(new ArrayList<>() {
                    {
                        add(ColumnHeader.deriveTargetColumnHeader(source, target, ColumnType.CLEARTEXT));
                        add(target);
                    }
                });
                add(new ArrayList<>() {
                    {
                        add(ColumnHeader.deriveTargetColumnHeader(source, target, ColumnType.FINGERPRINT));
                        add(target);
                    }
                });
                add(new ArrayList<>() {
                    {
                        add(ColumnHeader.deriveTargetColumnHeader(source, target, ColumnType.SEALED));
                        add(target);
                    }
                });
                add(new ArrayList<>() {
                    {
                        add(ColumnHeader.deriveTargetColumnHeader(source, target, null));
                        add(target);
                    }
                });
                add(new ArrayList<>() {
                    {
                        add(ColumnHeader.deriveTargetColumnHeader(source, null, ColumnType.CLEARTEXT));
                        add(source);
                    }
                });
                add(new ArrayList<>() {
                    {
                        add(ColumnHeader.deriveTargetColumnHeader(source, null, ColumnType.FINGERPRINT));
                        add(new ColumnHeader(source + ColumnHeader.DEFAULT_FINGERPRINT_SUFFIX));
                    }
                });
                add(new ArrayList<>() {
                    {
                        add(ColumnHeader.deriveTargetColumnHeader(source, null, ColumnType.SEALED));
                        add(new ColumnHeader(source + ColumnHeader.DEFAULT_SEALED_SUFFIX));
                    }
                });
                add(new ArrayList<>() {
                    {
                        add(ColumnHeader.deriveTargetColumnHeader(source, null, null));
                        add(null);
                    }
                });
                add(new ArrayList<>() {
                    {
                        add(ColumnHeader.deriveTargetColumnHeader(null, target, ColumnType.CLEARTEXT));
                        add(target);
                    }
                });
                add(new ArrayList<>() {
                    {
                        add(ColumnHeader.deriveTargetColumnHeader(null, target, ColumnType.FINGERPRINT));
                        add(target);
                    }
                });
                add(new ArrayList<>() {
                    {
                        add(ColumnHeader.deriveTargetColumnHeader(null, target, ColumnType.SEALED));
                        add(target);
                    }
                });
                add(new ArrayList<>() {
                    {
                        add(ColumnHeader.deriveTargetColumnHeader(null, target, null));
                        add(target);
                    }
                });
            }
        };

        for (int i = 0; i < cases.size(); i++) {
            final List<ColumnHeader> pair = cases.get(i);
            assertEquals(2, pair.size());
            assertEquals(pair.get(0), pair.get(1), "On case index " + i + " value calculated " + pair.get(0) + " does not match " +
                    pair.get(1) + ".");
        }
    }
}
