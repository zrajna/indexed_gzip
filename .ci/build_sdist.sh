#!/usr/bin/env bash

set -e

python -m pip install build
python -m build --sdist
