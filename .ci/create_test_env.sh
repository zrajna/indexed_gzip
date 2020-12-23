#!/usr/bin/env bash

# Set up a virtual environment with build and
# run-time dependencies for indexed_gzip.
#

set -e

envdir="$1"

# NUMPY=<some numpy version>
if [[ -n "$NUMPY" ]]; then
  NUMPY="numpy==$NUMPY"
else
  NUMPY="numpy"
fi

# NIBABEL=<some nibabel version>
if [[ -n "$NIBABEL" ]]; then
  NIBABEL="nibabel==$NIBABEL"
else
  NIBABEL="nibabel"
fi

python -m pip install --upgrade pip wheel virtualenv

if [[ "$PYTHON_VERSION" == "2.7" ]]; then
  virtualenv "$envdir"
else
  python -m venv "$envdir"
fi

source "$envdir"/bin/activate || source "$envdir"/Scripts/activate

pip install cython pytest coverage pytest-cov "$NUMPY" "$NIBABEL"
