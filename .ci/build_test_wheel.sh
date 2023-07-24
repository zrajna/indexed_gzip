#!/usr/bin/env bash
#
# Builds a binary wheel for indexed_gzip

set -e

envdir="$1"
thisdir=$(cd $(dirname "$0") && pwd)

source $thisdir/activate_env.sh "$envdir"

pip install --upgrade pip wheel setuptools twine build

python -m build

twine check dist/*
