#!/usr/bin/env bash
#
# Build a test version of indexed_gzip.

set -e

envdir="$1"
thisdir=$(cd $(dirname "$0") && pwd)

source $thisdir/activate_env.sh "$envdir"

# enable line tracing for cython
# modules - see setup.py
export INDEXED_GZIP_TESTING=1

pip install -e .
