// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.utils;

import java.util.regex.Pattern;

public final class StringTestUtility {

    private StringTestUtility() {
    }

    /**
     * Counts how many times a search string occurs (non-overlapping) in given string content.
     *
     * @param searchString String to search for
     * @param content      Content to search in
     * @return The number of occurrences of the search string in the content.
     */
    public static int countMatches(final String searchString, final String content) {
        return content.split(Pattern.quote(searchString), -1).length - 1;
    }
}
