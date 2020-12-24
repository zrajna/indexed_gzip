#!/usr/bin/env bash

set -e

curl -o zlib.tar.gz https://www.zlib.net/zlib-1.2.11.tar.gz

tar -xzf zlib.tar.gz
pushd zlib-1.2.11

if [[ "$PLATFORM" == "windows"* ]]; then
  mkdir build
  pushd build
  cmake ..
  cmake --build .
  ls -l zlibstatic.dir
  export ZLIB_HOME=$(pwd)/zlibstatic.dir
  popd
else
  CFLAGS=-fPIC ./configure --static
  export ZLIB_HOME=$(pwd)
  ls -l
fi

popd
