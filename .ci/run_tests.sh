#!/bin/bash
#
# Set up a virtual environment with dependencies,
# then build indexed_gzip and run its unit tests.
#

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

# NITERS=<number of iterations for some tests>
# (see conftest.py)
if [[ -n "$NITERS" ]]; then
  NITERS="--niters $NITERS"
fi

# NELEMS=<number of elements/size of
#         test file, for some tests>
# (see conftest.py)
if [[ -n "$NELEMS" ]]; then
  NELEMS="--nelems $NELEMS"
fi

python -m pip install --upgrade pip virtualenv
python -m venv test.venv

source ./test.venv/bin/activate

pip install cython pytest coverage pytest-cov "$NUMPY" "$NIBABEL"

# enable line tracing for cython
# modules - see setup.py
export INDEXED_GZIP_TESTING=1

python setup.py develop

pytest -v -s              \
       -m "$TEST_SUITE"   \
       -k "$TEST_PATTERN" \
       $NELEMS            \
       $NITERS            \
       $EXTRA_ARGS
