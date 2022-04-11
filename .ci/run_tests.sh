#!/bin/bash
#
# Run indexed_gzip unit tests. Assumes that
# python setup.py develop has been run.

set -e

envdir="$1"
thisdir=$(cd $(dirname "$0") && pwd)

source $thisdir/activate_env.sh "$envdir"

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

# No coverage on windows, because coverage or
# pytest-cov seem to have trouble with threading/
# multiproc, which causes the coverage report
# generation to sporadically fail
#
# https://github.com/pytest-dev/pytest-cov/issues/406
#
# Disabling coverage on all platforms, as it
# seems to be causing problems
EXTRA_ARGS="$EXTRA_ARGS --no-cov"

#       --cov-config=./.coveragerc \
python -m indexed_gzip.tests      \
       -c setup.cfg               \
       -v -s                      \
       -m "$TEST_SUITE"           \
       -k "$TEST_PATTERN"         \
       $NELEMS                    \
       $NITERS                    \
       $EXTRA_ARGS
