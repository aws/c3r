// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cleanrooms;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.With;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
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
@Slf4j
@Getter
@NoArgsConstructor(force = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class CleanRoomsDao {
    /**
     * Create a connection to AWS Clean Rooms.
     */
    private CleanRoomsClient client;

    /**
     * AWS CLI named profile to use with the AWS SDK.
     */
    @With
    private final String profile;

    /**
     * AWS region to use with the AWS SDK.
     */
    @With
    private final String region;

    /**
     * Construct an CleanRoomsDao with default specified settings.
     *
     * @param profile AWS CLI named profile to use with the AWS SDK
     * @param region AWS region to use with the AWS SDK
     */
    @Builder
    private CleanRoomsDao(final String profile, final String region) {
        this.profile = profile;
        this.region = region;
    }

    /**
     * Get the {@link AwsCredentialsProvider} to use for connecting with AWS Clean Rooms,
     * based on a specified named profile or the default provider.
     *
     * @return A {@link AwsCredentialsProvider} based on the specified named profile (if any).
     */
    AwsCredentialsProvider initAwsCredentialsProvider() {
        if (profile == null) {
            return DefaultCredentialsProvider.builder().build();
        } else {
            return ProfileCredentialsProvider.builder().profileName(profile).build();
        }
    }

    /**
     * Get the {@link Region} to use for connecting with AWS Clean Rooms,
     * based on a specified region or the default provider chain.
     *
     * @return A specified {@link Region} or the default.
     */
    Region initRegion() {
        if (region == null) {
            return DefaultAwsRegionProviderChain.builder().build().getRegion();
        } else {
            return Region.of(region);
        }
    }

    /**
     * Get the {@link CleanRoomsClient} for this instance, initializing it if it is not yet created.
     *
     * @return The instances {@link CleanRoomsClient}
     * @throws C3rRuntimeException If an SDK error occurs setting up the {@link CleanRoomsClient}
     */
    CleanRoomsClient getClient() {
        if (client != null) {
            return client;
        }
        try {
            client = CleanRoomsClient.builder()
                    .region(initRegion())
                    .credentialsProvider(initAwsCredentialsProvider())
                    .build();
            return client;
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
            response = getClient().getCollaboration(request);
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
