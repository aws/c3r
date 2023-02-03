// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cleanrooms;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.cleanrooms.CleanRoomsClient;
import software.amazon.awssdk.services.cleanrooms.model.AccessDeniedException;
import software.amazon.awssdk.services.cleanrooms.model.CleanRoomsException;
import software.amazon.awssdk.services.cleanrooms.model.Collaboration;
import software.amazon.awssdk.services.cleanrooms.model.DataEncryptionMetadata;
import software.amazon.awssdk.services.cleanrooms.model.GetCollaborationRequest;
import software.amazon.awssdk.services.cleanrooms.model.GetCollaborationResponse;
import software.amazon.awssdk.services.cleanrooms.model.ResourceNotFoundException;
import software.amazon.awssdk.services.cleanrooms.model.ThrottlingException;
import software.amazon.awssdk.services.cleanrooms.model.ValidationException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CleanRoomsDaoTest {

    private String oldAwsRegion;

    @BeforeEach
    public void setup() {
        oldAwsRegion = System.getProperty("aws.region");
    }

    @AfterEach
    public void teardown() {
        if (oldAwsRegion != null) {
            System.setProperty("aws.region", oldAwsRegion);
            oldAwsRegion = null;
        } else {
            System.clearProperty("aws.region");
        }
    }

    @Test
    public void constructorTest() throws CleanRoomsException {
        // Check that the constructor throws an error when AWS_REGION is set to the empty string
        System.setProperty("aws.region", "");
        assertThrows(C3rRuntimeException.class, () -> new CleanRoomsDao());
    }

    @Test
    public void getCollaborationDataEncryptionMetadataTest() throws CleanRoomsException {
        final ClientSettings expectedClientSettings = ClientSettings.builder()
                .allowCleartext(true)
                .allowDuplicates(true)
                .allowJoinsOnColumnsWithDifferentNames(false)
                .preserveNulls(false)
                .build();
        final DataEncryptionMetadata metadata = DataEncryptionMetadata.builder()
                .allowCleartext(expectedClientSettings.isAllowCleartext())
                .allowDuplicates(expectedClientSettings.isAllowDuplicates())
                .allowJoinsOnColumnsWithDifferentNames(expectedClientSettings.isAllowJoinsOnColumnsWithDifferentNames())
                .preserveNulls(expectedClientSettings.isPreserveNulls())
                .build();
        final Collaboration collaboration = Collaboration.builder()
                .dataEncryptionMetadata(metadata)
                .build();
        final GetCollaborationResponse response = GetCollaborationResponse.builder()
                .collaboration(collaboration)
                .build();
        final var client = mock(CleanRoomsClient.class);
        when(client.getCollaboration(any(GetCollaborationRequest.class))).thenReturn(response);

        final var cleanRoomsDaoDao = new CleanRoomsDao(client);
        final var actualClientSettings = cleanRoomsDaoDao.getCollaborationDataEncryptionMetadata(GeneralTestUtility.EXAMPLE_SALT
                .toString());
        assertEquals(expectedClientSettings, actualClientSettings);
    }

    @Test
    public void getCollaborationNullDataEncryptionMetadataTest() throws CleanRoomsException {
        final Collaboration collaboration = Collaboration.builder()
                .dataEncryptionMetadata((DataEncryptionMetadata) null)
                .build();
        final GetCollaborationResponse response = GetCollaborationResponse.builder()
                .collaboration(collaboration)
                .build();
        final var client = mock(CleanRoomsClient.class);
        when(client.getCollaboration(any(GetCollaborationRequest.class))).thenReturn(response);

        final var cleanRoomsDaoDao = new CleanRoomsDao(client);
        assertThrows(C3rRuntimeException.class, () ->
                cleanRoomsDaoDao.getCollaborationDataEncryptionMetadata(GeneralTestUtility.EXAMPLE_SALT.toString()));

        assertThrows(C3rRuntimeException.class, () ->
                cleanRoomsDaoDao.getCollaborationDataEncryptionMetadata(GeneralTestUtility.EXAMPLE_SALT.toString()));
    }

    @Test
    public void getCollaborationDataEncryptionMetadataFailureTest() throws CleanRoomsException {
        final List<Throwable> exceptions = List.of(
                ResourceNotFoundException.builder().message("ResourceNotFoundException").build(),
                AccessDeniedException.builder().message("AccessDeniedException").build(),
                ThrottlingException.builder().message("ThrottlingException").build(),
                ValidationException.builder().message("ValidationException").build(),
                CleanRoomsException.builder().message("CleanRoomsException").build(),
                SdkException.builder().message("SdkException").build());
        for (var exception : exceptions) {
            final var client = mock(CleanRoomsClient.class);
            final var cleanRoomsDaoDao = new CleanRoomsDao(client);
            when(client.getCollaboration(any(GetCollaborationRequest.class))).thenThrow(exception);
            assertThrows(C3rRuntimeException.class, () ->
                    cleanRoomsDaoDao.getCollaborationDataEncryptionMetadata(GeneralTestUtility.EXAMPLE_SALT.toString()));
            try {
                cleanRoomsDaoDao.getCollaborationDataEncryptionMetadata(GeneralTestUtility.EXAMPLE_SALT.toString());
                fail();
            } catch (Exception e) {
                assertEquals(exception, e.getCause());
            }
        }
    }
}
