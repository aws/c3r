#!/usr/bin/env bash

if [[ $# -ne 2 ]]; then
    echo "usage: $0 JAR DIR"
    echo "  where "
    echo "    JAR is the absolute path to the executable JAR archive"
    echo "    DIR is the absolute path of the test directory with the JAR test case"
    exit 1
fi

absolute_path() {
    echo "$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
}

JAR=$1
TEST_DIR=$2

if [[ ! -d $TEST_DIR ]]; then
    echo "$TEST_DIR is not a directory!"
    exit 1
fi
TEST_DIR="$(absolute_path "$TEST_DIR")"

if [[ ! -f $JAR ]]; then
    echo "$JAR is not a JAR file!"
    exit 1
fi
JAR="$(absolute_path "$JAR")"

echo "TEST: $TEST_DIR"
echo "  JAR:"
echo "    $JAR"

pushd $TEST_DIR > /dev/null

TEST_COMMANDS="[PASS|FAIL|SKEY]"

if [[ ! -f args.txt ]]; then
    echo "Expected a file args.txt with each line containing a test command: $TEST_COMMANDS"
    exit 1
fi

export C3R_SHARED_SECRET=""

run_jar() {
    # Set region to where test collaborations live
    export AWS_REGION=us-east-1
    echo "  Executing command:"
    echo "    java -jar $@"
    java -jar $@ >> stdout_stderr.log 2>&1
}

# Read args.txt line by line, executing each test command and asserting
# the dictacted PASS/FAIL exit code was returned.
while IFS= read -r line || [[ -n "$line" ]]; do
    # Trim whitespace
    line=`echo $line | xargs`
    # Skip blank lines or comment lines
    if [[ -e $line || $line = \#* ]]; then
        continue
    fi
    # "PASS", "FAIL", or "SKEY" begins the line.
    TEST_COMMAND=${line:0:4}
    # Trim whitespace again and drop leading PASS/FAIL/SKEY
    JAR_ARGS=`echo ${line:4} | xargs`
    if [[ $TEST_COMMAND == "SKEY" ]]; then
        export C3R_SHARED_SECRET="$JAR_ARGS"
        echo "  C3R_SHARED_SECRET"
        echo "    $C3R_SHARED_SECRET"
    elif [[ $TEST_COMMAND == "PASS" ]]; then
        run_jar $JAR $JAR_ARGS
        if [[ 0 -ne $? ]]; then
            echo "TEST FAILED: got a non-zero exit code."
            exit 1
        fi
    elif [[ $TEST_COMMAND == "FAIL" ]]; then
        run_jar $JAR $JAR_ARGS
        if [[ 0 -eq $? ]]; then
            echo "TEST FAILED: expected a non-0 exit code."
            exit 1
        fi
    else
        echo "TEST FAILED: each line in args.txt must begin with $TEST_COMMANDS, but found: $line"
        exit 1
    fi
done < args.txt

popd > /dev/null # leave $TEST_DIR

echo "TEST PASSED!"
exit 0
