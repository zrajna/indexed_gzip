#!/usr/bin/env bash

choco install python2 vcredist2008
choco install --ignore-dependencies vcpython27

echo "CC=C:\Program Files (x86)\Common Files\Microsoft\Visual C++ for Python\9.0\VC\Bin\amd64\cl.exe" >> "$GITHUB_ENV"
