#
# Tests for the indexed_gzip module.
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#

from __future__ import print_function

import               os
import os.path    as op
import itertools  as it
import subprocess as sp
import               sys
import               time
import               random
import               struct
import               hashlib

import numpy as np

import indexed_gzip as igzip


from . import ctest_zran


def read_element(gzf, element, seek=True):

    if seek:
        gzf.seek(element * 8)

    bytes = gzf.read(8)
    val   = np.ndarray(1, np.uint64, buffer=bytes)

    return val[0]
    


def test_open_close(testfile, nelems, seed):

    f = igzip.IndexedGzipFile(filename=testfile)

    try:
        element = np.random.randint(0, nelems, 1)
        readval = read_element(f, element)

        assert readval == element

    finally:
        f.close()

    assert f.closed()


def test_open_close_ctxmanager(testfile, nelems, seed):

    with igzip.IndexedGzipFile(filename=testfile) as f:

        element = np.random.randint(0, nelems, 1)
        readval = read_element(f, element)

    assert readval == element
    assert f.closed()


def test_create_from_open_handle(testfile, nelems, seed):

    f   = open(testfile, 'rb')
    gzf = igzip.IndexedGzipFile(fid=f)

    element = np.random.randint(0, nelems, 1)
    readval = read_element(gzf, element)

    gzf.close()

    try:

        assert readval == element
        assert gzf.closed()
        assert not f.closed

    finally:
        f.close()


def test_read_all(testfile, nelems, use_mmap):

    if use_mmap:
        print('WARNING: skipping test_read_all test, '
              'as it will require to much memory')
        return

    with igzip.IndexedGzipFile(filename=testfile) as f:
        data = f.read(nelems * 8)

    data = np.ndarray(shape=nelems, dtype=np.uint64, buffer=data)

    # Check that every value is valid
    assert ctest_zran.check_data_valid(data, 0)


def test_seek_and_read(testfile, nelems, niters, seed):

    with igzip.IndexedGzipFile(filename=testfile) as f: 
        
        # Pick some random elements and make
        # sure their values are all right
        seekelems = np.random.randint(0, nelems, niters)
        
        for i, testval in enumerate(seekelems):

            readval = read_element(f, testval)

            ft = f.tell()

            assert ft      == (testval + 1) * 8
            assert readval == testval
