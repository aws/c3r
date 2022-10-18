#!/usr/bin/env bash

set -e

if [ "$#" -ne 1 ]; then
    echo "Expected a top-level directory name for unsigned artifacts."
    exit 1
fi

ARTIFACT_DIR=$1

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

pushd $SCRIPT_DIR/.. > /dev/null

C3R_CLI_DIR=c3r-cli/build/libs

# Build everything
gradle wrapper
./gradlew build -x checkFileUtilTest

# Get the version from the CLI
C3R_VERSION=$(java -jar $C3R_CLI_DIR/c3r-cli-all.jar --version)
echo "JAR version found: $C3R_VERSION"

# Test CLI JAR
# ./c3r-cli/src/integration-test/test_all.sh $C3R_CLI_DIR/c3r-cli-all.jar
echo "!! WARNING !!"
echo "AUTOMATIC INTEGRATION TESTING NOT YET ENABLED"
echo "MANUALLY VERIFY CLI JAR USING ./c3r-cli/src/integration-test/test_all.sh"
echo "!! END OF WARNING !!"

mkdir -p $ARTIFACT_DIR

# Move CLI jar to the artifact directory
mv $C3R_CLI_DIR/c3r-cli-all.jar $ARTIFACT_DIR/c3r-cli-$C3R_VERSION.jar

# Given an SDK project name, collect it's JAR, javadocs JAR, and sources JAR
# and move them to the artifact directory with a version number applied
function move_sdk_jars() {
    local sdk_name=$1
    local sdk_jar_dir=$sdk_name/build/libs
    # Move sdk JARs to the artifact directory
    mv $sdk_jar_dir/$sdk_name.jar $ARTIFACT_DIR/$sdk_name-$C3R_VERSION.jar
    mv $sdk_jar_dir/$sdk_name-javadoc.jar $ARTIFACT_DIR/$sdk_name-javadoc-$C3R_VERSION.jar
    mv $sdk_jar_dir/$sdk_name-sources.jar $ARTIFACT_DIR/$sdk_name-sources-$C3R_VERSION.jar
}

# Move sdk JARs to the artifact directory
move_sdk_jars c3r-sdk-core
move_sdk_jars c3r-sdk-parquet

echo "JAR files tagged with version $C3R_VERSION and placed in $ARTIFACT_DIR/"

popd > /dev/null # $SCRIPT_DIR/..