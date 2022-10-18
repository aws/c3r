#!/usr/bin/env bash

INTEGRATION_TEST_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

absolute_path() {
    echo "$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
}

DEFAULT_JAR_LOC="$(absolute_path "$INTEGRATION_TEST_DIR/../../build/libs/c3r-cli-all.jar")"

if [[ $# -eq 0 ]]; then
    JAR=$DEFAULT_JAR_LOC
elif [[ $# -eq 1 ]]; then
    JAR=$1
else
    echo "usage: $0 [JAR]"
    echo "  where "
    echo "    JAR is the (optional) path to the executable JAR archive."
    echo "    If JAR is not provided, $DEFAULT_JAR_LOC is used."
    exit 1
fi



JAR="$(absolute_path "$JAR")"

TEST_COUNT=0
FAILURE_COUNT=0

run_single_test() {
    "$@"
    if [[ $? -ne 0 ]]; then 
        FAILURE_COUNT=$(($FAILURE_COUNT+1));
    fi
    TEST_COUNT=$(($TEST_COUNT+1));
}

echo "Running JAR integration tests with $JAR"

for TEST_DIR in $INTEGRATION_TEST_DIR/*; do
    if [[ -d $TEST_DIR ]]; then
        echo ""
        TEST_DIR="$(absolute_path "$TEST_DIR")"
        run_single_test $INTEGRATION_TEST_DIR/test_single.sh $JAR $TEST_DIR
    fi
done

if [[ $FAILURE_COUNT -gt 0 ]]; then
    echo "$FAILURE_COUNT out of $TEST_COUNT tests failed!"
    exit 1
fi

echo ""
echo "All $TEST_COUNT tests passed!"
exit 0
