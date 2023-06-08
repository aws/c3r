// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.spark.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StringTestUtilityTest {

    @Test
    public void countMatchesTest() {
        assertEquals(0, StringTestUtility.countMatches("a", ""));
        assertEquals(0, StringTestUtility.countMatches("a", "b"));
        assertEquals(1, StringTestUtility.countMatches("a", "a"));
        assertEquals(1, StringTestUtility.countMatches("a", "abcd"));
        assertEquals(3, StringTestUtility.countMatches("a", "abcdabcdabcd"));
        assertEquals(3, StringTestUtility.countMatches("aa", "aaabcdaaabcdaaabcd"));
    }
}
