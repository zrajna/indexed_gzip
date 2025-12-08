#!/usr/bin/env bash

set -e

python -m pip install --uprade pip
python -m pip install --uprade build
python -m build --sdist
