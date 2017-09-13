#!/bin/bash
#
# This script is called via .travis.yml. It is not intended
# to be called in any other manner.
#

# Get the path to this script
script_dir=`dirname $0`
pushd $script_dir > /dev/null
script_dir=`pwd`
pushd .. > /dev/null
igzip_dir=`pwd`
popd     > /dev/null
popd     > /dev/null

# 32 bit platform test has to be run in a docker container
if [ "$TEST_SUITE" == "32bittest" ]; then

    PYTHON_VERSION=$(python --version 2>&1)
    PYTHON_VERSION=${PYTHON_VERSION#* }

    docker run --rm \
           -e PYTHON_VERSION="$PYTHON_VERSION" \
           -v $igzip_dir:/indexed_gzip \
           32bit/ubuntu:16.04 \
           /indexed_gzip/.ci/run_32bit_test.sh

# Run standard test suite
else
    python setup.py develop;
    python setup.py test --addopts "-v -s -m \"$TEST_SUITE\" -k \"$TEST_PATTERN\" --nelems \"$NELEMS\" --niters \"$NITERS\" $EXTRA_ARGS";
fi
