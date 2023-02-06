// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cleanrooms;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.cleanrooms.CleanRoomsClient;
import software.amazon.awssdk.services.cleanrooms.model.AccessDeniedException;
import software.amazon.awssdk.services.cleanrooms.model.DataEncryptionMetadata;
import software.amazon.awssdk.services.cleanrooms.model.GetCollaborationRequest;
import software.amazon.awssdk.services.cleanrooms.model.GetCollaborationResponse;
import software.amazon.awssdk.services.cleanrooms.model.ResourceNotFoundException;
import software.amazon.awssdk.services.cleanrooms.model.ThrottlingException;
import software.amazon.awssdk.services.cleanrooms.model.ValidationException;

import java.util.function.Function;

/**
 * Create a connection to AWS Clean Rooms to get collaboration information.
 */
@AllArgsConstructor
@Slf4j
public class CleanRoomsDao {
    /**
     * Create a connection to AWS Clean Rooms.
     */
    private final CleanRoomsClient client;

    /**
     * Construct an CleanRoomsDao using the default CleanRoomsClient.
     *
     * @throws C3rRuntimeException If a {@link SdkException} is raised connecting to AWS Clean Rooms
     */
    public CleanRoomsDao() {
        try {
            client = CleanRoomsClient.create();
        } catch (SdkException e) {
            throw new C3rRuntimeException("Unable to connect to AWS Clean Rooms: " + e.getMessage(), e);
        }
    }

    /**
     * Get the cryptographic rules governing a particular collaboration.
     *
     * @param collaborationId Clean Room Collaboration Identification number
     * @return Cryptographic settings in use for the collaboration
     * @throws C3rRuntimeException If DataEncryptionMetadata cannot be retrieved from AWS Clean Rooms
     */
    public ClientSettings getCollaborationDataEncryptionMetadata(final String collaborationId) {
        final GetCollaborationRequest request = GetCollaborationRequest.builder()
                .collaborationIdentifier(collaborationId)
                .build();
        final String baseError = "Unable to retrieve the collaboration configuration for CollaborationID: `" + collaborationId + "`.";
        final String endError = "Please verify that the CollaborationID is correct and try again.";
        final GetCollaborationResponse response;
        try {
            response = client.getCollaboration(request);
        } catch (ResourceNotFoundException e) {
            throw new C3rRuntimeException(baseError + " No collaboration found. " + endError, e);
        } catch (AccessDeniedException e) {
            throw new C3rRuntimeException(baseError + " Access denied. " + endError, e);
        } catch (ThrottlingException e) {
            throw new C3rRuntimeException(baseError + " Throttling. Please wait a moment before trying again.", e);
        } catch (ValidationException e) {
            throw new C3rRuntimeException(baseError + " CollaborationID could not be validated. " + endError, e);
        } catch (SdkException e) {
            throw new C3rRuntimeException(baseError + " Unknown error: " + e.getMessage(), e);
        }
        final DataEncryptionMetadata metadata = response.collaboration().dataEncryptionMetadata();
        if (metadata == null) {
            throw new C3rRuntimeException(
                    "The collaboration with CollaborationID `" + collaborationId + "` was not created for use with " +
                            "C3R! C3R must be enabled on the collaboration when it's created in order to continue.");
        }
        final var settings = ClientSettings.builder()
                .allowJoinsOnColumnsWithDifferentNames(metadata.allowJoinsOnColumnsWithDifferentNames())
                .allowCleartext(metadata.allowCleartext())
                .allowDuplicates(metadata.allowDuplicates())
                .preserveNulls(metadata.preserveNulls())
                .build();

        final Function<Boolean, String> boolToYesOrNo = (b) -> b ? "yes" : "no";

        log.debug("Cryptographic computing parameters found for collaboration {}:", collaborationId);
        log.debug("  * Allow cleartext columns = {}",
                boolToYesOrNo.apply(settings.isAllowCleartext()));
        log.debug("  * Allow duplicates = {}",
                boolToYesOrNo.apply(settings.isAllowDuplicates()));
        log.debug("  * Allow JOIN of columns with different names = {}",
                boolToYesOrNo.apply(settings.isAllowJoinsOnColumnsWithDifferentNames()));
        log.debug("  * Preserve NULL values = {}",
                boolToYesOrNo.apply(settings.isPreserveNulls()));

        return settings;
    }
}
