#!/usr/bin/env bash

apt-get install -y                 \
        build-essential            \
        software-properties-common \
        zlib1g                     \
        zlib1g-dev

if [ "$PYTHON_VERSION" == "2.7" ]; then
  PACKAGES="python-pip python-virtualenv"
else
  PACKAGES="python3-pip python-${PYTHON_VERSION}-venv"
fi

add-apt-repository -y ppa:deadsnakes/ppa
apt-get update     -y
apt-get install    -y               \
        python"$PYTHON_VERSION"     \
        python"$PYTHON_VERSION"-dev \
        $PACKAGES
