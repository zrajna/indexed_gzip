#!/usr/bin/env bash
#
# Build a test version of indexed_gzip.

set -e

envdir="$1"

source "$envdir"/bin/activate || source "$envdir"/Scripts/activate

# enable line tracing for cython
# modules - see setup.py
export INDEXED_GZIP_TESTING=1

python setup.py develop
