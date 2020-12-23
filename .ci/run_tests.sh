#!/bin/bash
#
# Run indexed_gzip unit tests

set -e

envdir="$1"

source "$envdir"/bin/activate

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

pytest -v -s              \
       -m "$TEST_SUITE"   \
       -k "$TEST_PATTERN" \
       $NELEMS            \
       $NITERS            \
       $EXTRA_ARGS
