#!/usr/bin/env bash

python -m pip install cibuildwheel==1.7.2

export CIBW_BEFORE_BUILD="pip install cython"

# ZLIB is compiled into indexed_gzip on Windows -
# see .ci/download_zlib.sh and setup.py
export CIBW_ENVIRONMENT_WINDOWS="ZLIB_HOME='$ZLIB_HOME'"

# Run quick test suite on built wheels
export CIBW_TEST_REQUIRES="pytest pytest-cov coverage numpy nibabel"
export CIBW_TEST_COMMAND="cp {project}/setup.cfg {project}/conftest.py .; pytest {project}/indexed_gzip/tests -m 'not slow_test'"

python -m cibuildwheel --output-dir ./dist
