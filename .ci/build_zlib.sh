#!/usr/bin/env bash

set -e

curl -o zlib.tar.gz https://www.zlib.net/zlib-1.2.11.tar.gz

tar -xzf zlib.tar.gz
pushd zlib-1.2.11

if [[ "$PLATFORM" == "windows"* ]]; then
  cmake -DCMAKE_BUILD_TYPE=Release .
  cmake --build .
else
  CFLAGS=-fPIC ./configure --static
  make
fi

ls -l


export ZLIB_HOME=$(pwd)
popd
