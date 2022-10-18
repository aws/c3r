// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.utils;

import org.junit.jupiter.api.Test;

import static com.amazonaws.c3r.utils.StringTestUtility.countMatches;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class StringTestUtilityTest {

    @Test
    public void countMatchesTest() {
        assertEquals(0, countMatches("a", ""));
        assertEquals(0, countMatches("a", "b"));
        assertEquals(1, countMatches("a", "a"));
        assertEquals(1, countMatches("a", "abcd"));
        assertEquals(3, countMatches("a", "abcdabcdabcd"));
        assertEquals(3, countMatches("aa", "aaabcdaaabcdaaabcd"));
    }
}
