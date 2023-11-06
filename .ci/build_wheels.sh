#!/usr/bin/env bash

set -e

# prevent any prompts when
# apt/yum installing tzdata
export DEBIAN_FRONTEND="noninteractive"
export TZ="Europe/London"

# Make sure zlib headers are available on linux
export CIBW_BEFORE_ALL_LINUX="yum install -y zlib-devel || apt-get install -y zlib1g-dev || apk add zlib-dev || true"

# ZLIB is compiled into indexed_gzip on windwos -
# see .ci/download_zlib.sh and setup.py
export CIBW_ENVIRONMENT_WINDOWS="ZLIB_HOME='$ZLIB_HOME'"

# Run quick test suite on built wheels.
export CIBW_TEST_REQUIRES="cython pytest numpy nibabel coverage cython-coverage pytest-cov"

# Disable pypy builds (reasons for doing this have been lost to
# history [GHA logs of failing builds deleted]).
export CIBW_SKIP="pp*"

# Skip i686 and aarch64 tests

#  - I have experienced hangs on these platforms,
#    which I traced to a trivial numpy operation -
#    "numpy.linalg.det(numpy.eye(3))".

#  - Numpy wheels are not available for these
#    platforms, so has to be compiled from source
#    during the build, which massively increases
#    build time and complexity.
#
# Skip py312 tests on Windows due to unresolved
# test failures.
export CIBW_TEST_SKIP="*i686* *aarch64* cp312-win*"

# Pytest makes it *very* awkward to run tests
# from an installed package, and still find/
# interpret a conftest.py file correctly.
echo '#!/usr/bin/env bash'                                                                           >  testcmd
echo 'cp $1/pyproject.toml .'                                                                        >> testcmd
echo 'python -m indexed_gzip.tests -c pyproject.toml --no-cov -m "not slow_test" -k "not test_zran"' >> testcmd
chmod a+x testcmd

export CIBW_TEST_COMMAND="bash {project}/testcmd {project}"

python -m pip install cibuildwheel
python -m cibuildwheel --output-dir ./dist
