#!/usr/bin/env bash

# prevent any prompts when
# apt/yum installing tzdata
export DEBIAN_FRONTEND="noninteractive"
export TZ="Europe/London"

apt-get install -y      \
        build-essential \
        wget gpg        \
        zlib1g          \
        zlib1g-dev

# software-properties-common (which provides the
# add-apt-repository command) cannot be installed on
# i386/20.04 due to broken package specifications.
# So here we add the deadsnakes repository by hand.
echo "deb https://ppa.launchpadcontent.net/deadsnakes/ppa/ubuntu/ focal main"        > /etc/apt/sources.list.d//deadsnakes-ubuntu-ppa-focal.list
echo "# deb-src https://ppa.launchpadcontent.net/deadsnakes/ppa/ubuntu/ focal main" >> /etc/apt/sources.list.d//deadsnakes-ubuntu-ppa-focal.list

wget -O - "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0xF23C5A6CF475977595C89F51BA6932366A755776" | \
  gpg --batch --yes --dearmor --output "/etc/apt/trusted.gpg.d/deadsnakes-ubuntu-ppa.gpg"

apt-get update     -y
apt-get install    -y                \
        python"$PYTHON_VERSION"      \
        python"$PYTHON_VERSION"-dev  \
        python${PYTHON_VERSION}-venv \
        python3-pip
