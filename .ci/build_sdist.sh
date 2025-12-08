#!/usr/bin/env bash

set -e

python -m pip install --upgrade pip
python -m pip install --upgrade build
python -m build --sdist
