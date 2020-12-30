#!/usr/bin/env bash

# Make sure cython is available on all platforms
# Numpy is required to build the test modules
export CIBW_BEFORE_BUILD="pip install cython numpy"

# Make sure zlib headers are available on linux
export CIBW_BEFORE_ALL_LINUX="yum install -y zlib-devel"

# ZLIB is compiled into indexed_gzip on windwos -
# see .ci/download_zlib.sh and setup.py
export CIBW_ENVIRONMENT_WINDOWS="ZLIB_HOME='$ZLIB_HOME'"

# Run quick test suite on built wheels. We need
# cython for the Cython.Coverage plugin.
export CIBW_TEST_REQUIRES="cython pytest pytest-cov coverage numpy nibabel"

echo "#!/usr/bin/env bash"                              > testcmd
echo "cp $1/.coveragerc $1/setup.cfg $1/conftest.py ." >> testcmd
echo "pytest -m 'not slow_test' --pyargs indexed_gzip" >> testcmd
chmod a+x testcmd

export CIBW_TEST_COMMAND="./testcmd {project}"

python -m pip install cibuildwheel==1.7.2

python -m cibuildwheel --output-dir ./dist
