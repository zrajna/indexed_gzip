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
#
# Disable musllinux builds until numpy binaries are available (as
# compiling numpy takes too long, and causes GHA jobs to time out).
#
# Disable py312 builds until numpy is available
export CIBW_SKIP="pp* *musllinux* *312*"

# Skip i686/aarch64 tests - I have experienced hangs on these
# platforms, which I traced to a trivial numpy operation -
# "numpy.linalg.det(numpy.eye(3))". This occurs when numpy has
# to be compiled from source during the build, so can be
# re-visited if/when numpy is avaialble on all platforms.
export CIBW_TEST_SKIP="*i686* *aarch64*"

# Pytest makes it *very* awkward to run tests
# from an installed package, and still find/
# interpret a conftest.py file correctly.
echo '#!/usr/bin/env bash'                                                        >  testcmd
echo 'cp $1/pyproject.toml .'                                                     >> testcmd
echo 'python -m indexed_gzip.tests -c pyproject.toml --no-cov -m "not slow_test"' >> testcmd
chmod a+x testcmd

export CIBW_TEST_COMMAND="bash {project}/testcmd {project}"

python -m pip install cibuildwheel
python -m cibuildwheel --output-dir ./dist
