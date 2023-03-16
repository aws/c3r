// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.cleanrooms;

import com.amazonaws.c3r.config.ClientSettings;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import com.amazonaws.c3r.utils.FileTestUtility;
import com.amazonaws.c3r.utils.GeneralTestUtility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.profiles.ProfileFileSystemSetting;
import software.amazon.awssdk.regions.Region;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class CleanRoomsDaoTest {

    private String origAwsRegion;

    private String origAwsAccessKeyId;

    private String origAwsSecretAccessKey;

    private final String testRegion = "not-real-test-region";

    @BeforeEach
    public void setup() {
        origAwsRegion = System.getProperty("aws.region");
        origAwsAccessKeyId = System.getProperty("aws.accessKeyId");
        origAwsSecretAccessKey = System.getProperty("aws.secretAccessKey");

        System.setProperty("aws.region", testRegion);
    }

    @AfterEach
    public void teardown() {
        if (origAwsRegion != null) {
            System.setProperty("aws.region", origAwsRegion);
            origAwsRegion = null;
        } else {
            System.clearProperty("aws.region");
        }
        if (origAwsAccessKeyId != null) {
            System.setProperty("aws.accessKeyId", origAwsAccessKeyId);
            origAwsAccessKeyId = null;
        } else {
            System.clearProperty("aws.accessKeyId");
        }
        if (origAwsSecretAccessKey != null) {
            System.setProperty("aws.secretAccessKey", origAwsSecretAccessKey);
            origAwsSecretAccessKey = null;
        } else {
            System.clearProperty("aws.secretAccessKey");
        }
    }

    @Test
    public void initAwsCredentialsProviderNoProfile() throws CleanRoomsException {
        final CleanRoomsDao dao = CleanRoomsDao.builder().build();
        assertInstanceOf(DefaultCredentialsProvider.class, dao.initAwsCredentialsProvider());
    }

    @Test
    public void initAwsCredentialsProviderCustomProfile() throws CleanRoomsException, IOException {
        // We set up a custom temporary config file with a profile `my-profile` and then
        // have the CleanRoomsDao use that, checking it got all the expected info
        final String myProfileName = "my-profile";
        final String myAccessKeyId = "AKIAI44QH8DHBEXAMPLE";
        final String mySecretAccessKey = "je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY";
        final String configFileContent = String.join("\n",
                "[default]",
                "aws_access_key_id=AKIAIOSFODNN7EXAMPLE",
                "aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "",
                "[profile " + myProfileName + "]",
                "aws_access_key_id=" + myAccessKeyId,
                "aws_secret_access_key=" + mySecretAccessKey,
                ""
        );

        final Path configFile = FileTestUtility.createTempFile();
        Files.write(configFile, configFileContent.getBytes(StandardCharsets.UTF_8));

        System.setProperty(ProfileFileSystemSetting.AWS_CONFIG_FILE.property(), configFile.toString());
        final CleanRoomsDao dao = CleanRoomsDao.builder().profile(myProfileName).build();
        final AwsCredentialsProvider credentialsProvider = dao.initAwsCredentialsProvider();
        assertInstanceOf(ProfileCredentialsProvider.class, credentialsProvider);
        final AwsCredentials credentials = credentialsProvider.resolveCredentials();
        assertEquals(myAccessKeyId, credentials.accessKeyId());
        assertEquals(mySecretAccessKey, credentials.secretAccessKey());
    }

    @Test
    public void initRegionDefaultRegion() throws CleanRoomsException {
        final CleanRoomsDao dao = CleanRoomsDao.builder().build();
        assertEquals(Region.of(testRegion), dao.initRegion());
    }

    @Test
    public void initRegionCustomRegion() throws CleanRoomsException {
        final String customRegion = "us-east-1";
        final CleanRoomsDao dao = CleanRoomsDao.builder().region(customRegion).build();
        assertEquals(Region.of(customRegion), dao.initRegion());
    }

    @Test
    public void initCleanRoomsClientSdkExceptionTest() throws CleanRoomsException {
        final CleanRoomsDao dao = mock(CleanRoomsDao.class);
        when(dao.initRegion()).thenThrow(SdkClientException.create("Region oops!"));
        when(dao.initAwsCredentialsProvider()).thenThrow(SdkClientException.create("Credentials oops!"));
        when(dao.getClient()).thenCallRealMethod();
        assertThrows(C3rRuntimeException.class, dao::getClient);
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

        final var dao = spy(CleanRoomsDao.class);
        when(dao.getClient()).thenReturn(client);
        final var actualClientSettings = dao.getCollaborationDataEncryptionMetadata(GeneralTestUtility.EXAMPLE_SALT
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

        final var dao = spy(CleanRoomsDao.class);
        when(dao.getClient()).thenReturn(client);
        assertThrows(C3rRuntimeException.class, () ->
                dao.getCollaborationDataEncryptionMetadata(GeneralTestUtility.EXAMPLE_SALT.toString()));

        assertThrows(C3rRuntimeException.class, () ->
                dao.getCollaborationDataEncryptionMetadata(GeneralTestUtility.EXAMPLE_SALT.toString()));
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
            when(client.getCollaboration(any(GetCollaborationRequest.class))).thenThrow(exception);
            final var dao = spy(CleanRoomsDao.class);
            when(dao.getClient()).thenReturn(client);
            assertThrows(C3rRuntimeException.class, () ->
                    dao.getCollaborationDataEncryptionMetadata(GeneralTestUtility.EXAMPLE_SALT.toString()));
            try {
                dao.getCollaborationDataEncryptionMetadata(GeneralTestUtility.EXAMPLE_SALT.toString());
                fail();
            } catch (Exception e) {
                assertEquals(exception, e.getCause());
            }
        }
    }
}
