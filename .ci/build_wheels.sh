#!/usr/bin/env bash

python -m pip install cython setuptools wheel
python -m pip install cibuildwheel==1.7.2

# ZLIB is compiled into indexed_gzip on Windows - see
# .ci/download_zlib.sh and setup.py
export CIBW_ENVIRONMENT_WINDOWS="ZLIB_HOME='$ZLIB_HOME'"

python -m cibuildwheel --output-dir ./dist
