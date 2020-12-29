#!/usr/bin/env bash
#
# Download zlib sources
#
set -e

curl -o zlib.tar.gz https://www.zlib.net/zlib-1.2.11.tar.gz

tar -xzf zlib.tar.gz

ZLIB_HOME=$(pwd)/zlib-1.2.11

# if windows, turn /drive/path/to/zlib into
# drive:/path/to/zlib. Only works for drives
# with a single letter
if [[ "$PLATFORM" == "windows"* ]]; then
  drive=$(echo "$ZLIB_HOME" | cut -d / -f 2)
  offset=$(expr ${#drive} + 2)
  ZLIB_HOME="${drive}:/${ZLIB_HOME:$offset}"
fi

echo "Setting ZLIB_HOME: $ZLIB_HOME"

# used by setup.py
echo "ZLIB_HOME=$ZLIB_HOME" >> "$GITHUB_ENV"
