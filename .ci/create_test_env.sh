#!/usr/bin/env bash

# Set up a virtual environment with build and
# run-time dependencies for indexed_gzip.
#

set -e

envdir="$1"

# NUMPY=<some numpy version>
if [[ -n "$NUMPY" ]]; then
  NUMPY="numpy=$NUMPY"
else
  NUMPY="numpy"
fi

# NIBABEL=<some nibabel version>
if [[ -n "$NIBABEL" ]]; then
  NIBABEL="nibabel=$NIBABEL"
else
  NIBABEL="nibabel"
fi

python -m pip install --upgrade pip virtualenv
python -m venv "$envdir"

source "$envdir"/bin/activate

pip install cython pytest coverage pytest-cov "$NUMPY" "$NIBABEL"
