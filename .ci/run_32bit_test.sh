#!/bin/bash
#
# This script is called via .travis.yml. It is not intended
# to be called in any other manner.
#

apt-get update
apt-get install -y libssl-dev openssl wget build-essential
cd /
wget https://www.python.org/ftp/python/3.5.3/Python-3.5.3.tgz
tar xf Python-3.5.3.tgz
cd Python-3.5.3
./configure --prefix=/python35
make && make install
cd /python35/bin
ln -s python3.5 python
ln -s pip3 pip

export PATH=/python35/bin:$PATH

pip install numpy pytest cython

cd /indexed_gzip
python setup.py develop
python setup.py test --addopts "-v -s --niters 500";
python setup.py test --addopts "-v -s --niters 500 --concat";
