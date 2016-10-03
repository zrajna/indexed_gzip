#
# Tests for the indexed_gzip module.
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#

from __future__ import print_function

import              os
import os.path   as op
import itertools as it
import              sys
import              gzip
import              time
import              random
import              struct
import              hashlib

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
    random   .seed(1234567)
    np.random.seed(1234567)

    if not op.exists(TEST_FILE):
        gen_test_data(TEST_FILE)

    random   .seed(1234567)
    np.random.seed(1234567)

        
def teardown_module():
    if op.exists(TEST_FILE):
        os.remove(TEST_FILE)


def gen_test_data(filename):
    """Make some data to test with. """
    
    print('Generating test data')

    start = time.time()

    # The file just contains a sequentially
    # increasing list of numbers
    with gzip.GzipFile(filename, 'wb') as f:

        toWrite    = TEST_FILE_NELEMS
        maxBufSize = 67108864
        index      = 0
        
        while toWrite > 0:

            nvals    = min(maxBufSize, toWrite)
            toWrite -= nvals

            vals     = np.arange(index, index + nvals, dtype=np.uint64)
            index   += nvals

            f.write(vals.tostring())

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

        print('{} == {}?'.format(element, readval))

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
