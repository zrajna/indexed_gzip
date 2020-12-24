#!/usr/bin/env bash

set -e

wget https://www.zlib.net/zlib-1.2.11.tar.gz

tar -xzf zlib-1.2.11.tar.gz
pushd zlib-1.2.11

CFLAGS=-fPIC ./configure --static

make

export ZLIB_HOME=$(pwd)
popd
