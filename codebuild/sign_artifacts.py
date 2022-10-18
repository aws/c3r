import os
import time
import sys
import botocore
import boto3
import shutil

MAX_WAIT_CYCLES = 180

# Get script variables
account_id = os.environ['SIGNER_ACCOUNT_ID']
access_role = os.environ['SIGNER_ARTIFACT_ACCESS_ROLE']
external_id = os.environ['EXTERNAL_ID']
profile_identifier = os.environ['SIGNER_PROFILE_IDENTIFIER']
platform_identifier = os.environ['SIGNER_PLATFORM_IDENTIFIER']
codebuild_id = os.environ['CODEBUILD_BUILD_ID'][-64:].replace(':', '-')

# Get the list of artifacts to sign
from os import listdir
from os.path import isfile, join
artifacts_to_sign = [f for f in listdir('unsigned-artifacts') if isfile(join('unsigned-artifacts', f))]
print("Artifacts found to sign: " + str(artifacts_to_sign))

# Assume the artifact access role
sts = boto3.client('sts')
role_arn = 'arn:aws:iam::' + account_id + ':role/' + access_role
print(f'Assuming role:')
print(f'          RoleArn: {role_arn}')
print(f'  RoleSessionName: {codebuild_id}')
print(f'       ExternalId: {external_id}')
assume_role_result = sts.assume_role(
    RoleArn = role_arn,
    RoleSessionName = codebuild_id,
    DurationSeconds = 3600,
    ExternalId = external_id
)
print("Assumed appropriate role for signing")

# Upload the artifacts
print("Starting to upload artifacts")
s3 = boto3.client(
    's3',
    aws_access_key_id = assume_role_result['Credentials']['AccessKeyId'],
    aws_secret_access_key = assume_role_result['Credentials']['SecretAccessKey'],
    aws_session_token = assume_role_result['Credentials']['SessionToken']
)
for artifact in artifacts_to_sign:
    with open('unsigned-artifacts/' + artifact, 'rb') as artifact_file:
        key = profile_identifier + '/' + platform_identifier + '/' + artifact
        bucket = account_id + '-unsigned-bucket'
        print(f'  Uploading: {artifact}')
        print(f'        Key: {key}')
        print(f'     Bucket: {bucket}')
        s3.put_object(
            ACL = 'bucket-owner-full-control',
            Bucket = bucket,
            Key = key,
            Body = artifact_file
        )
print("Upload complete")

# Get the tags for the artifact
print("Starting to poll for signer-job-id tags")
signer_job_id_map = {}
current_wait_cycle = 1
artifacts_to_fetch = artifacts_to_sign.copy()
while len(signer_job_id_map) != len(artifacts_to_sign) and current_wait_cycle < MAX_WAIT_CYCLES:
    artifacts_to_remove = []
    for artifact in artifacts_to_fetch:
        get_object_tagging_result = s3.get_object_tagging(
            Bucket = account_id + '-unsigned-bucket',
            Key = profile_identifier + '/' + platform_identifier + '/' + artifact
        )
        for tag in get_object_tagging_result['TagSet']:
            if tag['Key'] == 'signer-job-id':
                artifacts_to_remove.append(artifact)
                signer_job_id_map[artifact] = tag['Value']
                print("\tTag " + tag['Value'] + " found for: " + artifact)
                break
    for artifact in artifacts_to_remove:
        artifacts_to_fetch.remove(artifact)
    current_wait_cycle += 1
    if len(signer_job_id_map) != len(artifacts_to_sign):
        time.sleep(10)
if len(signer_job_id_map) != len(artifacts_to_sign):
    sys.exit('Not all items were appropriately tagged, the following were not: ' + str(artifacts_to_fetch))
print("All tags found")

# Poll the destination bucket until the signed items are made available
print("Starting to poll for signed artifacts")
current_wait_cycle = 1
artifacts_to_fetch = []
shutil.rmtree('signed-artifacts', ignore_errors=True)
os.mkdir('signed-artifacts')
for item in signer_job_id_map.items():
    isJar = item[0].endswith('.jar')
    if isJar:
        artifacts_to_fetch.append(item[0] + '-' + item[1] + '.jar')
    else:
        artifacts_to_fetch.append(item[0] + '-' + item[1])
while artifacts_to_fetch and current_wait_cycle < MAX_WAIT_CYCLES:
    artifacts_to_remove = []
    for artifact in artifacts_to_fetch:
        try:
            bucket = account_id + '-signed-bucket'
            key = profile_identifier + '/' + platform_identifier + '/' + artifact
            print(f'  Downloading: {artifact}')
            print(f'          Key: {key}')
            print(f'       Bucket: {bucket}')
            get_object_result = s3.get_object(Bucket = bucket, Key = key)
            signed_artifact_file_name = artifact[:artifact.index('.jar') + 4].replace('-unsigned', '-signed')
            with open('signed-artifacts/' + signed_artifact_file_name, 'wb') as artifact_file:
                artifact_file.write(get_object_result['Body'].read())
            artifacts_to_remove.append(artifact)
            print("Signed artifact downloaded for: " + artifact)
        except botocore.exceptions.ClientError as exn:
            pass
    for artifact in artifacts_to_remove:
        artifacts_to_fetch.remove(artifact)
    current_wait_cycle += 1
    if artifacts_to_fetch:
        time.sleep(10)
if current_wait_cycle >= MAX_WAIT_CYCLES:
    print('Maximum wait cycles reached while downloading artifacts!')
if artifacts_to_fetch:
    sys.exit('Not all items were able to be downloaded, the following were not: ' + str(artifacts_to_fetch))
print("All signed artifacts downloaded!")