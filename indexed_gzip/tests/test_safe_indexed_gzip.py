#!/usr/bin/env python
#
# test_safe_indexed_gzip.py -
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#

from __future__ import print_function
from __future__ import division

import sys

import threading

import numpy as np

import pytest

import indexed_gzip as igzip

from . import check_data_valid

pytestmark = pytest.mark.indexed_gzip_test


def test_SafeIndexedGzipFile_open_close(testfile):

    f = igzip.SafeIndexedGzipFile(filename=testfile)
    f.seek(10)
    f.read(10)
    f.close()


def test_SafeIndexedGzipFile_pread_threaded(testfile, nelems):

    filesize     = nelems * 8
    indexSpacing = max(524288, filesize // 2000)

    with igzip.SafeIndexedGzipFile(filename=testfile,
                                   spacing=indexSpacing) as f:

        readelems = 50
        readsize  = readelems * 8
        nthreads  = 100
        allreads  = []

        def do_pread(nbytes, offset):
            data = f.pread(nbytes, int(offset * 8))
            allreads.append((offset, data))

        offsets = np.linspace(0, nelems - readelems, nthreads,
                              dtype=np.uint64)
        threads = [threading.Thread(target=do_pread, args=(readsize, o))
                   for o in offsets]
        [t.start() for t in threads]
        [t.join()  for t in threads]

        assert len(allreads) == nthreads
        for offset, data in allreads:

            assert len(data) == readsize

            data = np.ndarray(shape=readelems, dtype=np.uint64,
                              buffer=data)
            assert check_data_valid(data, offset, offset + readelems)
