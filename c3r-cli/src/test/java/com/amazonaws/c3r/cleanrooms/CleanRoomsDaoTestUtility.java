// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cleanrooms;

import org.mockito.stubbing.Answer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public final class CleanRoomsDaoTestUtility {

    /**
     * Hidden utility class constructor.
     */
    private CleanRoomsDaoTestUtility() {
    }

    public static CleanRoomsDao generateMockDao() {
        final CleanRoomsDao mockCleanRoomsDao = org.mockito.Mockito.mock(CleanRoomsDao.class);
        when(mockCleanRoomsDao.withProfile(any())).thenAnswer((Answer<CleanRoomsDao>) (invocation) -> {
            when(mockCleanRoomsDao.getProfile()).thenReturn(invocation.getArgument(0));
            return mockCleanRoomsDao;
        });
        when(mockCleanRoomsDao.withRegion(any())).thenAnswer((Answer<CleanRoomsDao>) (invocation) -> {
            when(mockCleanRoomsDao.getRegion()).thenReturn(invocation.getArgument(0));
            return mockCleanRoomsDao;
        });
        when(mockCleanRoomsDao.getRegion()).thenCallRealMethod();
        return mockCleanRoomsDao;
    }
}
