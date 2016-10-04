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

# 2**30 values, at 8 bytes each, is 8GB
# 2**29 values, at 8 bytes each, is 4GB
# 2**28 values, at 8 bytes each, is 2GB
# 2**27 values, at 8 bytes each, is 1GB
TEST_FILE_NELEMS = 2**29 + 1
TEST_FILE_SIZE   = TEST_FILE_NELEMS * 8
TEST_FILE        = 'ctest_indexed_gzip_testdata.gz'


def setup_module():

    if not op.exists(TEST_FILE):
        gen_test_data(TEST_FILE)

    seed = np.random.randint(2 ** 32)
    np.random.seed(seed)

    print('Random seed for tests: {}'.format(seed))

        
def teardown_module():
    if op.exists(TEST_FILE):
        os.remove(TEST_FILE)


def gen_test_data(filename):
    """Make some data to test with. """
    
    start = time.time()

    # The file just contains a sequentially
    # increasing list of numbers

    # maxBufSize is in elements, *not* in bytes
    toWrite    = TEST_FILE_NELEMS
    maxBufSize = 33554432
    index      = 0

    print('Generating test data ({} bytes -> {})'.format(
        toWrite * 8,
        filename))

    with open(filename, 'wb') as f:

        i = 0

        while toWrite > 0:

            nvals    = min(maxBufSize, toWrite)
            toWrite -= nvals

            vals     = np.arange(index, index + nvals, dtype=np.uint64)
            vals     = vals.tostring()
            index   += nvals

            print('Generated block {} ({} values, {} bytes)'.format(
                i,
                nvals,
                len(vals)))

            proc = sp.Popen(['gzip', '-c'], stdin=sp.PIPE, stdout=f)
            proc.communicate(input=vals)

            print('Written block {} ({} bytes to go)'.format(
                i,
                toWrite))
            i += 1

    end = time.time()

    print('Done in {:0.2f} seconds'.format(end - start))


def read_element(gzf, element, seek=True):

    if seek:
        gzf.seek(element * 8)

    bytes = gzf.read(8)
    val   = np.ndarray(1, np.uint64, buffer=bytes)

    return val[0]
    


def test_open_close():

    f = igzip.IndexedGzipFile(filename=TEST_FILE)

    try:
        element = np.random.randint(0, TEST_FILE_NELEMS, 1)
        readval = read_element(f, element)

        assert readval == element

    finally:
        f.close()

    assert f.closed()


def test_open_close_ctxmanager():

    with igzip.IndexedGzipFile(filename=TEST_FILE) as f:

        element = np.random.randint(0, TEST_FILE_NELEMS, 1)
        readval = read_element(f, element)

    assert readval == element
    assert f.closed()


def test_create_from_open_handle():

    f   = open(TEST_FILE, 'rb')
    gzf = igzip.IndexedGzipFile(fid=f)

    element = np.random.randint(0, TEST_FILE_NELEMS, 1)
    readval = read_element(gzf, element)

    gzf.close()

    try:

        assert readval == element
        assert gzf.closed()
        assert not f.closed

    finally:
        f.close()


def test_read_all(niters=5000):

    with igzip.IndexedGzipFile(filename=TEST_FILE) as f:
        data = f.read(TEST_FILE_SIZE)

    data = np.ndarray(shape=TEST_FILE_NELEMS, dtype=np.uint64, buffer=data)

    # Pick some random elements and make
    # sure their values are all right
    for testval in np.random.randint(0, TEST_FILE_NELEMS, niters):
        assert data[testval] == testval


def test_seek_and_read(niters=5000):

    with igzip.IndexedGzipFile(filename=TEST_FILE) as f:
        
        # Pick some random elements and make
        # sure their values are all right
        for testval in np.random.randint(0, TEST_FILE_NELEMS, niters):
            readval = read_element(f, testval)
            
            assert readval == testval 
