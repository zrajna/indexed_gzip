#!/usr/bin/env bash

python -m pip install cibuildwheel==1.7.2

# Make sure cython is available on all platforms
export CIBW_BEFORE_BUILD="pip install cython"

# Make sure zlib headers are available on linux
export CIBW_BEFORE_ALL_LINUX="yum install -y zlib-devel"

# ZLIB is compiled into indexed_gzip on windwos -
# see .ci/download_zlib.sh and setup.py
export CIBW_ENVIRONMENT_WINDOWS="ZLIB_HOME='$ZLIB_HOME'"

# Run quick test suite on built wheels. We need
# cython for the Crthon.Coverage plugin.
export CIBW_TEST_REQUIRES="cython pytest pytest-cov coverage numpy nibabel"
PREP="cp {project}/.coveragerc {project}/setup.cfg {project}/conftest.py ."
DBG="ls; python -c 'import indexed_gzip.tests as t; import os; import os.path as op; print(os.listdir(op.dirname(t.__file__)))'"
RUN="pytest -m 'not slow_test' --pyargs indexed_gzip --import-mode=append"
export CIBW_TEST_COMMAND="$PREP; $DBG; $RUN"

python -m cibuildwheel --output-dir ./dist
