// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.json;

// Tests common to all schema types regarding schema generation to and from json.
interface TableSchemaCommonTypeAdapterTestInterface {
    // Read JSON file representing child class in as {@code TableSchema} and verify values are correct.
    void readSchemaAsTableSchemaWithValueValidationTest();

    // Read JSON file representing child class in as child class and verify values are correct.
    void readSchemaAsActualClassWithValueValidationTest();

    // Read JSON representing child class in as another child class type.
    void readAsWrongSchemaClassTypeTest();

    // Read JSON null value into the child class and make sure {@code null} is returned.
    void readNullJsonInputTest();

    // Read empty JSON object {@code {}} in to child class and verify it fails validation.
    void readWithEmptyJsonClassTest();

    // Verify that the wrong JSON type fails to be read in to child class.
    void readWithWrongJsonClassTest();

    // Verify that if the headerRow value indicates the other class type, the child class fails validation as not initialized.
    void readWithWrongHeaderValueTest();

    // Verify that if columns are set to {@code null} for the child class, construction fails.
    void readWithJsonNullAsColumnsTest();

    // Verify that if the wrong JSON type is used for columns the child class fails to be created.
    void readWithWrongJsonTypeAsColumnsValueTest();

    // Verify that if the columns are empty in all ways they could be for the child class, construction fails.
    void verifyEmptyColumnsRejectedTest();

    // Verify that the child class is written out with the correct JSON fields when it is written as a {@code TableSchema}.
    void writeSchemaAsTableSchemaTest();

    // Verify the child class is written out with all the correct JSON fields when it is written as itself.
    void writeSchemaAsActualClassTest();

    /*
     * Verify that writing the child class to JSON fails when the wrong type is specified,
     * regardless of whether the object is referred to as a {@code TableSchema} or the child class.
     */
    void writeSchemaAsWrongClassTest();

    // Verify that the child class writes the {@code null} value out to disk correctly when called as {@code TableSchema} or itself.
    void writeNullJsonInputTest();

    /*
     * Round trip verification test.
     * Starting with a JSON string, verify the same JSON string is returned when:
     * - Reading it in as a {@code TableSchema} and writing it out as a {@code TableSchema}
     * - Reading it in as the child class and writing it out as a {@code TableSchema}
     * - Reading it in as a {@code TableSchema} and writing it out as the child class
     * - Reading it in as the child class and writing it out as the child class
     */
    void roundTripStringToStringSerializationTest();

    /*
     * Round trip verification test.
     * Starting with an instance of the child class, verify the same value is returned when:
     * - Writing it out as a {@code TableSchema} and reading it in as a {@code TableSchema}
     * - Writing it out as the child class and reading it in as a {@code TableSchema}
     * - Writing it out as a {@code TableSchema} and reading it in as the child class
     * - Writing it out as the child class and reading it in as the child class
     */
    void roundTripClassToClassSerializationTest();
}