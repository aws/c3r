// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.config;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/*
 * This test class is for maintaining the schema specification rules. This is why reflection is used to modify some values in tests.
 * These tests care that the schema dealt correctly with unspecified values or unique parameter rules. We are concerned with throwing
 * errors for missing required values, handling the few default value cases correctly and handling required unspecified values correctly.
 * Defaults related to value normalization are checked as well as selecting control conditions based off of settings.
 */
public class ColumnSchemaTest {
    // Canary information to ensure we catch changes to the schema
    private static final int NUMBER_OF_COLUMN_SETTINGS = 5;

    private static final int NUMBER_OF_PAD_TYPES = 3;

    private static final int NUMBER_OF_COLUMN_TYPES = 3;

    /*
     * When creating a source column header programmatically, ensure leading and trailing white space is removed. If the remaining value
     * is empty, the value should be rejected.
     */
    @Test
    public void whiteSpaceInSourceHeaderTest() {
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GeneralTestUtility.fingerprintColumn("   "));
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GeneralTestUtility.fingerprintColumn("\n"));
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GeneralTestUtility.fingerprintColumn("\t \n"));
        assertEquals(new ColumnHeader("test"), GeneralTestUtility.fingerprintColumn(" test \t").getSourceHeader());
    }

    /*
     * When creating a target column header and the value is specified, the leading and trailing white space should be removed. If the
     * result is empty, alert the user with an error as they may have intended to have a value. If there is a non-empty value, use that
     * as the target column header name.
     */
    @Test
    public void whiteSpaceInTargetHeaderTest() {
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GeneralTestUtility.fingerprintColumn("NA", "   "));
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GeneralTestUtility.fingerprintColumn("NA", "\n"));
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> GeneralTestUtility.fingerprintColumn("NA", "\t \n"));
        assertEquals(new ColumnHeader("test"), GeneralTestUtility.fingerprintColumn("NA", "\ntest").getTargetHeader());
    }

    // If a column is cleartext and cleartext, it should not require preprocessing.
    @Test
    public void requiresPreprocessingFalseTest() {
        final ColumnSchema columnSchema = GeneralTestUtility.cleartextColumn("source");
        assertFalse(columnSchema.requiresPreprocessing());
    }

    // If a column type is fingerprint, preprocessing should be required.
    @Test
    public void requiresPreprocessingAllowDuplicatesTest() {
        final ColumnSchema columnSchema = GeneralTestUtility.fingerprintColumn("source");
        assertTrue(columnSchema.requiresPreprocessing());
    }

    // If a column is encrypted then preprocessing is required.
    @Test
    public void requiresPreprocessingNotCleartextTest() {
        final ColumnSchema columnSchema = GeneralTestUtility.sealedColumn("source");
        assertTrue(columnSchema.requiresPreprocessing());
    }

    // If a column is encrypted and has a pad, preprocessing is required.
    @Test
    public void requiresPreprocessingWithPaddingTest() {
        final ColumnSchema columnSchema = GeneralTestUtility.sealedColumn("source", "source", PadType.FIXED, 1);
        assertTrue(columnSchema.requiresPreprocessing());
    }

    /*
     * This test is a canary for the schema changing. If we change the configuration options in the future, we'll either be adding
     * parameters or removing parameters. This test will alert us as a reminder to update the tests on schema configuration definition
     * rules. We should see three versions explicitly declared. One is actually the Object() constructor here and isn't actually
     * reachable, but it is detected as declared. Then we have the copy constructor and the declared constructor for ColumnSchema. The
     * number of parameters for ColumnSchema should match the CURRENT_PARAMETER_COUNT declared as part of this class. That value changing
     * should set off the canary that we need to update this class for the new schema rules.
     */
    @Test
    public void validateConstructorParameterCountsCanaryTest() {
        final Class<?> colObj = ColumnSchema.class;
        final Constructor<?>[] colCons = colObj.getDeclaredConstructors();
        // Check to make sure we only have the expected constructors:
        // Object() (though the empty constructor isn't accessible)
        // ColumnSchema(ColumnSchema) copy constructor
        for (Constructor<?> con : colCons) {
            assert (con.getParameterCount() == 0 || con.getParameterCount() == 1 ||
                    con.getParameterCount() == NUMBER_OF_COLUMN_SETTINGS);
            if (con.getParameterCount() == 0) {
                assert con.isSynthetic();
            } else if (con.getParameterCount() == 1) {
                assertEquals("public com.amazonaws.c3r.config.ColumnSchema(com.amazonaws.c3r.config.ColumnSchema)", con.toString());
            } else {
                final String signature = "private com.amazonaws.c3r.config.ColumnSchema(com.amazonaws.c3r.config.ColumnHeader," +
                        "com.amazonaws.c3r.config.ColumnHeader,com.amazonaws.c3r.config.ColumnHeader,com.amazonaws.c3r.config.Pad," +
                        "com.amazonaws.c3r.config.ColumnType)";
                assertEquals(signature, con.toString());
            }
        }
    }

    /*
     * This test is a canary for the pad types changing. If we change the configuration options in the future, we'll either be adding,
     * removing or renaming options. This test will alert us as a reminder to update the tests on schema configuration definition
     * rules.
     */
    @Test
    public void validatePadTypeOptionsCanaryTest() {
        assertEquals(NUMBER_OF_PAD_TYPES, PadType.values().length);
        assertEquals(PadType.FIXED, PadType.valueOf("FIXED"));
        assertEquals(PadType.MAX, PadType.valueOf("MAX"));
        assertEquals(PadType.NONE, PadType.valueOf("NONE"));
    }

    /*
     * This test is a canary for the column types changing. If we change the configuration options in the future, we'll either be adding,
     * removing or renaming options. This test will alert us as a reminder to update the tests on schema configuration definition
     * rules.
     */
    @Test
    public void validateColumnTypeOptionsCanaryTest() {
        assertEquals(NUMBER_OF_COLUMN_TYPES, ColumnType.values().length);
        assertEquals(ColumnType.FINGERPRINT, ColumnType.valueOf("FINGERPRINT"));
        assertEquals(ColumnType.SEALED, ColumnType.valueOf("SEALED"));
        assertEquals(ColumnType.CLEARTEXT, ColumnType.valueOf("CLEARTEXT"));
    }


    // This test verifies that the copy constructor does not propagate bad state data for when the SDK is made public.
    @Test
    public void verifyCopyConstructorFailsOnBadSchemaTest() {
        // Use reflection to make an invalid Column without a source name
        final ColumnSchema c = GeneralTestUtility.cleartextColumn("s", "t");

        // Check bad pad is not propagated
        try {
            final Field targetField = c.getClass().getDeclaredField("targetHeader");
            targetField.setAccessible(true);
            targetField.set(c, new ColumnHeader("t"));
            final Pad p = Pad.builder().type(PadType.MAX).length(100).build();
            final Field padLen = p.getClass().getDeclaredField("length");
            padLen.setAccessible(true);
            padLen.set(p, -100);
            final Field pad = c.getClass().getDeclaredField("pad");
            pad.setAccessible(true);
            pad.set(c, p);
        } catch (NoSuchFieldException | SecurityException | IllegalAccessException e) {
            fail("Error mutating target column header or pad using reflection");
        }
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> new ColumnSchema(c));

        // Check that bad pad/type combo is not propagated
        try {
            final Field pad = c.getClass().getDeclaredField("pad");
            pad.setAccessible(true);
            pad.set(c, Pad.builder().type(PadType.MAX).length(100).build());
        } catch (NoSuchFieldException | SecurityException | IllegalAccessException e) {
            fail("Error mutating pad using reflection");
        }
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> new ColumnSchema(c));
    }

    @Test
    public void padTypeNoneCannotHaveNonZeroLengthTest() {
        // Test that Pad type none does not accept a length
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> Pad.builder().type(PadType.NONE).length(32).build());
    }

    @Test
    public void padLengthCannotBeOutOfRangeTest() {
        // Assert that out of range values are not accepted
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> Pad.builder().type(PadType.MAX).length(-10).build());
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> Pad.builder().type(PadType.MAX).length(17000).build());
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> Pad.builder().type(PadType.FIXED).length(-10).build());
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> Pad.builder().type(PadType.FIXED).length(17000).build());
    }

    @Test
    public void columnMissingTypeFailTest() {
        // Test that Pad type none does not accept a length
        assertThrowsExactly(C3rIllegalArgumentException.class, () ->
                ColumnSchema.builder()
                        .sourceHeader(new ColumnHeader("source"))
                        .targetHeader(new ColumnHeader("target"))
                        .type(null)
                        .build());
    }

    @Test
    public void columnMissingPadFailTest() {
        // Test that Pad type none does not accept a length
        assertThrowsExactly(C3rIllegalArgumentException.class, () ->
                ColumnSchema.builder()
                        .sourceHeader(new ColumnHeader("source"))
                        .targetHeader(new ColumnHeader("target"))
                        .type(ColumnType.SEALED)
                        .pad(null)
                        .build());
    }

    @Test
    public void columnTypeToStringTest() {
        assertEquals("fingerprint", ColumnType.FINGERPRINT.toString());
        assertEquals("sealed", ColumnType.SEALED.toString());
        assertEquals("cleartext", ColumnType.CLEARTEXT.toString());
    }
}
