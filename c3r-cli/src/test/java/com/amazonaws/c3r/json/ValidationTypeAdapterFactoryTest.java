// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.json;

import com.amazonaws.c3r.config.ColumnHeader;
import com.amazonaws.c3r.config.ColumnSchema;
import com.amazonaws.c3r.config.ColumnType;
import com.amazonaws.c3r.config.Pad;
import com.amazonaws.c3r.config.PadType;
import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ValidationTypeAdapterFactoryTest {

    /*
     * This class is used when we want to test the same value entered as a JSON string and entered programmatically.
     * The same behavior is expected for the jsonValue and the javaValue.
     */
    private static final class JavaJsonPair {
        private final String jsonValue;

        private final Object javaValue;

        /*
         * Create a schema parameter, store it as an object along with the equivalent JSON string.
         * The JSON string should be insertable inside a larger JSON schema definition without modification
         * (i.e., no trailing commas or scoping).
         */
        private JavaJsonPair(final String jsonValue, final Object javaValue) {
            this.jsonValue = jsonValue;
            this.javaValue = javaValue;
        }
    }

    // What follows are all the basic valid options for each schema parameter

    // Source Column Headers
    private final JavaJsonPair src = new JavaJsonPair(
            "\"sourceHeader\":\"source\"", new ColumnHeader("source"));

    // Target Column Headers
    private final JavaJsonPair tgt = new JavaJsonPair(
            "\"targetHeader\":\"target\"", new ColumnHeader("Target"));

    // Pad values
    private final JavaJsonPair padFixed = new JavaJsonPair(
            "\"pad\":{ \"type\":\"fixed\", \"length\":32 }", Pad.builder().type(PadType.FIXED).length(32).build());

    private final JavaJsonPair padMax = new JavaJsonPair(
            "\"pad\":{ \"type\":\"max\", \"length\":3200 }", Pad.builder().type(PadType.MAX).length(3200).build());

    private final JavaJsonPair padNone = new JavaJsonPair(
            "\"pad\":{ \"type\":\"none\" }", Pad.DEFAULT);

    // Column types
    private final JavaJsonPair typeFingerprint = new JavaJsonPair(
            "\"type\":\"fingerprint\"", ColumnType.FINGERPRINT);

    private final JavaJsonPair typeSealed = new JavaJsonPair(
            "\"type\":\"sealed\"", ColumnType.SEALED);

    private final JavaJsonPair typeCleartext = new JavaJsonPair(
            "\"type\":\"cleartext\"", ColumnType.CLEARTEXT);

    // JSON field separator
    private final String sep = ",\n";

    private ColumnSchema decodeColumn(final String content) {
        return GsonUtil.fromJson(content, ColumnSchema.class);

    }

    /*
     * When creating a source column header via a JSON configuration file, white space should be removed from the start and end of the
     * string. If the remaining string is empty, the value should be rejected.
     */
    @Test
    public void whiteSpaceInJsonSourceHeaderTest() {
        assertThrows(C3rIllegalArgumentException.class, () ->
                decodeColumn(
                        "{ \"sourceHeader\":\"\", \"type\":\"fingerprint\" }"
                ));
        assertThrows(C3rIllegalArgumentException.class, () ->
                decodeColumn(
                        "{ \"sourceHeader\":\"   \", \"type\":\"fingerprint\" }"
                ));
        assertThrows(C3rIllegalArgumentException.class, () ->
                decodeColumn(
                        "{ \"sourceHeader\":\"\t\t\", \"type\":\"fingerprint\" }"
                ));
        assertThrows(C3rIllegalArgumentException.class, () ->
                decodeColumn(
                        "{ \"sourceHeader\":\"\t \", \"type\":\"fingerprint\" }"
                ));
        final ColumnSchema c = decodeColumn(
                "{ \"sourceHeader\":\" test column \t\", \"type\":\"fingerprint\" }"
        );
        assertEquals(new ColumnHeader("test column"), c.getSourceHeader());
    }

    /*
     * When creating a target column header and the value is specified, the leading and trailing white space should be removed. If the
     * result is empty, alert the user with an error as they may have intended to have a value. If there is a non-empty value, use that
     * as the target column header name. Even if the user specifies the empty string, we will still throw en error because they actually
     * wrote in a value. We only inherit from the source column header when the target column header is unspecified.
     */
    @Test
    public void whiteSpaceInJsonTargetHeaderTest() {
        assertThrows(C3rIllegalArgumentException.class, () ->
                decodeColumn(
                        "{\n" + src.jsonValue + sep + "\"targetHeader\":\"   \"" + sep + typeFingerprint + "\n}"));
        assertThrows(C3rIllegalArgumentException.class, () ->
                decodeColumn(
                        "{\n" + src.jsonValue + sep + "\"targetHeader\":\"\"" + sep + typeFingerprint + "\n}"));
        final ColumnSchema c = decodeColumn(
                "{\n" + src.jsonValue + sep + "\"targetHeader\":\"test\t \"" + sep + typeFingerprint.jsonValue + "\n}");
        assertEquals(new ColumnHeader("test"), c.getTargetHeader());
    }

    // Verify that the target header value is respected if specified.
    @Test
    public void checkTargetHeaderIsSpecifiedIsAcceptedTest() {
        // Test specified value for target header
        final ColumnSchema c1java =
                ColumnSchema.builder().sourceHeader((ColumnHeader) src.javaValue).targetHeader((ColumnHeader) tgt.javaValue)
                        .pad((Pad) padMax.javaValue).type((ColumnType) typeSealed.javaValue).build();
        assertEquals(tgt.javaValue, c1java.getTargetHeader());
        assertNotEquals(src.javaValue, c1java.getTargetHeader());

        final ColumnSchema c1json = decodeColumn(
                "{\n" + src.jsonValue + sep + tgt.jsonValue + sep + padMax.jsonValue + sep +
                        typeSealed.jsonValue + "\n}");
        assertEquals(c1java.getTargetHeader(), c1json.getTargetHeader());
        assertNotEquals(c1json.getSourceHeader(), c1json.getTargetHeader());
    }

    // Confirm that Sealed accepts all pad types, does not set a default value for the padding and doesn't accept unspecified padding.
    @Test
    public void checkColumnPadRequiredWithSealedTypeTest() {
        // Fixed Pad is accepted
        assertDoesNotThrow(() -> {
            final ColumnSchema columnSchema =
                    ColumnSchema.builder().sourceHeader((ColumnHeader) src.javaValue).targetHeader((ColumnHeader) tgt.javaValue)
                            .pad((Pad) padFixed.javaValue).type((ColumnType) typeSealed.javaValue).build();
            assertEquals(padFixed.javaValue, columnSchema.getPad());
        });

        assertDoesNotThrow(() -> {
            final ColumnSchema columnSchema = decodeColumn(
                    "{\n" + src.jsonValue + sep + tgt.jsonValue + sep + padFixed.jsonValue + sep +
                            typeSealed.jsonValue + "\n}");
            assertEquals(padFixed.javaValue, columnSchema.getPad());
        });

        // Max Pad
        assertDoesNotThrow(() -> {
            final ColumnSchema columnSchema =
                    ColumnSchema.builder().sourceHeader((ColumnHeader) src.javaValue).targetHeader((ColumnHeader) tgt.javaValue)
                            .pad((Pad) padMax.javaValue).type((ColumnType) typeSealed.javaValue).build();
            assertEquals(padMax.javaValue, columnSchema.getPad());
        });

        assertDoesNotThrow(() -> {
            final ColumnSchema columnSchema = decodeColumn(
                    "{\n" + src.jsonValue + sep + tgt.jsonValue + sep + padMax.jsonValue + sep +
                            typeSealed.jsonValue + "\n}");
            assertEquals(padMax.javaValue, columnSchema.getPad());
        });

        // No Pad
        assertDoesNotThrow(() -> {
            final ColumnSchema columnSchema =
                    ColumnSchema.builder().sourceHeader((ColumnHeader) src.javaValue).targetHeader((ColumnHeader) tgt.javaValue)
                            .pad((Pad) padNone.javaValue).type((ColumnType) typeSealed.javaValue).build();
            assertEquals(padNone.javaValue, columnSchema.getPad());
        });

        assertDoesNotThrow(() -> {
            final ColumnSchema columnSchema = decodeColumn(
                    "{\n" + src.jsonValue + sep + tgt.jsonValue + sep + padNone.jsonValue + sep +
                            typeSealed.jsonValue + "\n}");
            assertEquals(padNone.javaValue, columnSchema.getPad());
        });

        // Check that pad must be specified for sealed
        assertThrows(C3rIllegalArgumentException.class, () ->
                ColumnSchema.builder().sourceHeader((ColumnHeader) src.javaValue).targetHeader((ColumnHeader) tgt.javaValue)
                        .type((ColumnType) typeSealed.javaValue).build());
        assertThrows(C3rIllegalArgumentException.class, () ->
                decodeColumn(
                        "{\n" + src.jsonValue + sep + tgt.jsonValue + sep + typeSealed.jsonValue + "\n}"));
    }

    // Test that other column types do not accept a pad type, including none.
    @Test
    public void padLengthMustNotBeSpecifiedForNonSealedTypesTest() {
        // Check that pad must be unspecified for Fingerprint type
        assertThrows(C3rIllegalArgumentException.class, () ->
                ColumnSchema.builder().sourceHeader((ColumnHeader) src.javaValue).targetHeader((ColumnHeader) tgt.javaValue)
                        .pad((Pad) padFixed.javaValue).type((ColumnType) typeFingerprint.javaValue).build());
        assertThrows(C3rIllegalArgumentException.class, () ->
                decodeColumn(
                        "{\n" + src.jsonValue + sep + tgt.jsonValue + sep + padFixed.jsonValue + sep +
                                typeFingerprint.jsonValue + "\n}"));

        assertThrows(C3rIllegalArgumentException.class, () ->
                ColumnSchema.builder().sourceHeader((ColumnHeader) src.javaValue).targetHeader((ColumnHeader) tgt.javaValue)
                        .pad((Pad) padNone.javaValue).type((ColumnType) typeFingerprint.javaValue).build());
        assertThrows(C3rIllegalArgumentException.class, () ->
                decodeColumn(
                        "{\n" + src.jsonValue + sep + tgt.jsonValue + sep + padNone.jsonValue + sep +
                                typeFingerprint.jsonValue + "\n}"));

        assertDoesNotThrow(() -> ColumnSchema.builder().sourceHeader((ColumnHeader) src.javaValue)
                .targetHeader((ColumnHeader) tgt.javaValue).type((ColumnType) typeFingerprint.javaValue).build());
        assertDoesNotThrow(() ->
                decodeColumn(
                        "{\n" + src.jsonValue + sep + tgt.jsonValue + sep + typeFingerprint.jsonValue + "\n}"));

        // Check that pad must be unspecified for Cleartext type
        assertThrows(C3rIllegalArgumentException.class, () ->
                ColumnSchema.builder().sourceHeader((ColumnHeader) src.javaValue).targetHeader((ColumnHeader) tgt.javaValue)
                        .pad((Pad) padFixed.javaValue).type((ColumnType) typeCleartext.javaValue).build());
        assertThrows(C3rIllegalArgumentException.class, () ->
                decodeColumn(
                        "{\n" + src.jsonValue + sep + tgt.jsonValue + sep + padFixed.jsonValue + sep +
                                typeCleartext.jsonValue + "\n}"));

        assertThrows(C3rIllegalArgumentException.class, () ->
                ColumnSchema.builder().sourceHeader((ColumnHeader) src.javaValue).targetHeader((ColumnHeader) tgt.javaValue)
                        .pad((Pad) padNone.javaValue).type((ColumnType) typeCleartext.javaValue).build());
        assertThrows(C3rIllegalArgumentException.class, () ->
                decodeColumn(
                        "{\n" + src.jsonValue + sep + tgt.jsonValue + sep + padNone.jsonValue + sep +
                                typeCleartext.jsonValue + "\n}"));

        assertDoesNotThrow(() -> ColumnSchema.builder().sourceHeader((ColumnHeader) src.javaValue)
                .targetHeader((ColumnHeader) tgt.javaValue).type((ColumnType) typeCleartext.javaValue).build());
        assertDoesNotThrow(() ->
                decodeColumn(
                        "{\n" + src.jsonValue + sep + tgt.jsonValue + sep + typeCleartext.jsonValue + "\n}"));
    }

    // Verify that a default value for column type is not set, and it must always be specified.
    @Test
    public void columnTypeMustBeSpecifiedTest() {
        // Test type unspecified is invalid
        assertThrows(C3rIllegalArgumentException.class, () ->
                ColumnSchema.builder().sourceHeader((ColumnHeader) src.javaValue).targetHeader((ColumnHeader) tgt.javaValue)
                        .pad((Pad) padFixed.javaValue).build());
        assertThrows(C3rIllegalArgumentException.class, () ->
                decodeColumn(
                        "{\n" + src.jsonValue + sep + tgt.jsonValue + sep + padFixed.jsonValue + "\n}"));
        assertThrows(C3rIllegalArgumentException.class, () ->
                ColumnSchema.builder().sourceHeader((ColumnHeader) src.javaValue).targetHeader((ColumnHeader) tgt.javaValue)
                        .pad((Pad) padNone.javaValue).build());
        assertThrows(C3rIllegalArgumentException.class, () ->
                decodeColumn(
                        "{\n" + src.jsonValue + sep + tgt.jsonValue + sep + padNone.jsonValue + "\n}"));
    }
}
