#!/usr/bin/env bash

# prevent any prompts when
# apt/yum installing tzdata
export DEBIAN_FRONTEND="noninteractive"
export TZ="Europe/London"

apt-get install -y                 \
        build-essential            \
        software-properties-common \
        zlib1g                     \
        zlib1g-dev

add-apt-repository -y ppa:deadsnakes/ppa
apt-get update     -y
apt-get install    -y                \
        python"$PYTHON_VERSION"      \
        python"$PYTHON_VERSION"-dev  \
        python${PYTHON_VERSION}-venv \
        python3-pip
