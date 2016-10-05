#
# Tests for the zran module.
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
import               hashlib

import numpy as np

from libc.stdio cimport (SEEK_SET, FILE, fdopen)

from cpython.mem cimport (PyMem_Malloc,
                          PyMem_Realloc,
                          PyMem_Free)

cimport zran


# 2**30 values, at 8 bytes each, is 8GB
# 2**29 values, at 8 bytes each, is 4GB
# 2**28 values, at 8 bytes each, is 2GB
# 2**27 values, at 8 bytes each, is 1GB
TEST_FILE_NELEMS = 2**28 + 1
TEST_FILE_SIZE   = TEST_FILE_NELEMS * 8
TEST_FILE        = 'ctest_zran_testdata.gz'


# TODO Run all tests on both concatenated
#      and single-stream gzip files


def setup_module():

    if not op.exists(TEST_FILE):
        gen_test_data(TEST_FILE)

    seed = np.random.randint(2 ** 32)
    np.random.seed(seed)


        
def teardown_module():
    if op.exists(TEST_FILE):
        os.remove(TEST_FILE)


def gen_test_data(filename, nelems=TEST_FILE_NELEMS, concat=False):
    """Make some data to test with. """
    
    start = time.time()

    # The file just contains a sequentially
    # increasing list of numbers

    # maxBufSize is in elements, *not* in bytes
    toWrite    = nelems

    if not concat: maxBufSize = nelems
    else:          maxBufSize = min(16777216, nelems / 50)
    
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


cdef read_element(zran.zran_index_t *index, element, seek=True):

    cdef void *buffer

    buf    = ReadBuffer(8)
    buffer = buf.buffer

    if seek:
        assert zran.zran_seek(index, element * 8, SEEK_SET, NULL) == 0
        assert zran.zran_tell(index) == element * 8

    assert zran.zran_read(index, buffer, 8) == 8
    assert zran.zran_tell(index) == element * 8 + 8

    pybuf = <bytes>(<char *>buffer)[:8]
    val   = np.ndarray(1, np.uint64, buffer=pybuf)

    return val[0]


cdef class ReadBuffer:
    """Wrapper around a chunk of memory.
 
    .. see:: http://docs.cython.org/src/tutorial/memory_allocation.html
    """

    cdef void *buffer
    """A raw chunk of bytes. """

    
    def __cinit__(self, size_t size):
        """Allocate ``size`` bytes of memory. """

        self.buffer = PyMem_Malloc(size);

        if not self.buffer:
            raise MemoryError('PyMem_Malloc fail')


    def resize(self, size_t size):
        """Re-allocate the memory to the given ``size``. """
        
        buf = PyMem_Realloc(self.buffer, size)

        if not buf:
            raise MemoryError('PyMem_Realloc fail')

        self.buffer = buf


    def __dealloc__(self):
        """Free the mwmory. """
        PyMem_Free(self.buffer)

    
def test_init():
    """Tests a bunch of permutations of the parameters to zran_init. """

    spacings      = [0, 16384, 32768, 65536, 524288, 1048576, 2097152, 4194304]
    window_sizes  = [0, 8192, 16384, 32768, 65536, 131072]
    readbuf_sizes = [0, 8192, 16384, 24576, 32768, 65536, 131072]
    flags         = [0, zran.ZRAN_AUTO_BUILD]

    cdef zran.zran_index_t index
    cdef FILE             *cfid

    with open(TEST_FILE, 'rb') as pyfid:
        
        cfid = fdopen(pyfid.fileno(), 'rb')

        for s, w, r, f in it.product(spacings,
                                     window_sizes,
                                     readbuf_sizes,
                                     flags):

            result = not zran.zran_init(&index, cfid, s, w, r, f)

            expected = True

            # zran_init should fail if the point spacing
            # is less than the window size

            if w == 0: w = 32768
            if s == 0: s = 1048576
            if r == 0: r = 16384

            expected = (w >= 32768) and (s > w)

            assert result == expected

            zran.zran_free(&index)


def test_init_file_modes():

    modes = ['r', 'r+', 'w', 'w+', 'a', 'a+']

    files = [TEST_FILE, TEST_FILE,
             'dummy.gz', 'dummy.gz', 'dummy.gz', 'dummy.gz']

    cdef zran.zran_index_t index
    cdef FILE             *cfid
    cdef bytes             bmode
    cdef char             *cmode

    for filename, mode in zip(files, modes):

        with open(filename, mode) as pyfid:

            bmode    = mode.encode()
            cmode    = bmode
            cfid     = fdopen(pyfid.fileno(), cmode)

            expected = mode == 'r'

            result = not zran.zran_init(&index, cfid, 0, 0, 0, 0)

            assert result == expected

            zran.zran_free(&index)

        if filename == 'dummy.gz' and op.exists(filename):
            os.remove(filename)


def test_seek_to_end():
    
    cdef zran.zran_index_t index

    seek_point   = TEST_FILE_SIZE - 1
    indexSpacing = max(524288, TEST_FILE_SIZE / 1500)

    with open(TEST_FILE, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)
            
        assert zran.zran_seek(&index, seek_point, SEEK_SET, NULL) == 0
        
        zt = zran.zran_tell(&index)
        
        assert zt == seek_point

        zran.zran_free(&index)


def test_seek_beyond_end():
    
    cdef zran.zran_index_t index

    seek_point   = TEST_FILE_SIZE + 10
    indexSpacing = max(524288, TEST_FILE_SIZE / 1500)

    with open(TEST_FILE, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)
            
        assert zran.zran_seek(&index, seek_point, SEEK_SET, NULL) == 0
        
        zt = zran.zran_tell(&index)

        assert zt == TEST_FILE_SIZE

        zran.zran_free(&index) 

        
def test_sequential_seek_to_end():
    
    cdef zran.zran_index_t index

    seek_points = np.linspace(0, TEST_FILE_SIZE, 5000, dtype=np.uint64)
    indexSpacing = max(524288, TEST_FILE_SIZE / 2000)

    with open(TEST_FILE, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)

        for sp in seek_points:
            
            assert zran.zran_seek(&index, sp, SEEK_SET, NULL) == 0
            
            zt = zran.zran_tell(&index)
        
            assert zt == sp

        zran.zran_free(&index) 


def test_random_seek():

    cdef zran.zran_index_t index

    seekpoints   = [random.randint(0, TEST_FILE_SIZE) for i in range(500)]
    indexSpacing = max(524288, TEST_FILE_SIZE / 1000)

    with open(TEST_FILE, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)

        for sp in seekpoints:
            
            assert zran.zran_seek(&index, sp, SEEK_SET, NULL) == 0

            zt = zran.zran_tell(&index)

            assert zt == sp

        zran.zran_free(&index)


def test_read_all():

    indexSpacing = max(524288, TEST_FILE_SIZE / 1000)
    
    cdef zran.zran_index_t index
    cdef void             *buffer

    buf    = ReadBuffer(TEST_FILE_SIZE) 
    buffer = buf.buffer

    with open(TEST_FILE, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)

        nbytes = zran.zran_read(&index, buffer, TEST_FILE_SIZE)

        assert nbytes                 == TEST_FILE_SIZE
        assert zran.zran_tell(&index) == nbytes
        
        zran.zran_free(&index)

    pybuf = <bytes>(<char *>buffer)[:nbytes]
    data  = np.ndarray(TEST_FILE_NELEMS, np.uint64, pybuf)

    for i, val in enumerate(data):
        assert val == i
            

def test_seek_then_read_to_end():
    
    indexSpacing = max(524288, TEST_FILE_SIZE / 1000)
    buf          = ReadBuffer(TEST_FILE_SIZE)
    seekelems    = np.linspace(0, TEST_FILE_NELEMS, 50, dtype=np.uint64)
    np.random.shuffle(seekelems)
    
    cdef zran.zran_index_t index
    cdef void             *buffer = buf.buffer

    with open(TEST_FILE, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)

        for se in seekelems:

            zran.zran_seek(&index, se * 8, SEEK_SET, NULL)

            nbytes = zran.zran_read(&index, buffer, TEST_FILE_SIZE)

            assert nbytes                 == (TEST_FILE_NELEMS - se) * 8
            assert zran.zran_tell(&index) == TEST_FILE_SIZE
            
            pybuf = <bytes>(<char *>buffer)[:nbytes]
            data  = np.ndarray(nbytes / 8, np.uint64, pybuf)

            for i, val in enumerate(data, se):
                assert val == i
                    
        zran.zran_free(&index)


def test_random_seek_and_read():

    cdef zran.zran_index_t index

    seekelems    = [random.randint(0, TEST_FILE_NELEMS) for i in range(10000)]
    indexSpacing = max(524288, TEST_FILE_SIZE / 1000)

    with open(TEST_FILE, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)

        for se in seekelems:

            val = read_element(&index, se, True)
            assert val == se

        zran.zran_free(&index) 


def test_read_all_sequential():

    cdef zran.zran_index_t index

    indexSpacing = max(524288, TEST_FILE_SIZE / 1000)

    # Takes too long to read all elements
    seekelems = np.linspace(0, TEST_FILE_NELEMS, 10000, dtype=np.uint64)

    with open(TEST_FILE, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)


        for se in seekelems:
            
            val = read_element(&index, se, False)
            assert val == se

        zran.zran_free(&index)


def test_build_then_read():
    
    indexSpacing = max(524288, TEST_FILE_SIZE / 1000)
    buf          = ReadBuffer(TEST_FILE_SIZE)
    seekelems    = np.linspace(0, TEST_FILE_NELEMS, 50, dtype=np.uint64)
    np.random.shuffle(seekelems) 
    
    cdef zran.zran_index_t index
    cdef void             *buffer = buf.buffer

    with open(TEST_FILE, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)

        assert not zran.zran_build_index(&index, 0, 0)

        for se in seekelems:

            assert zran.zran_seek(&index, se * 8, SEEK_SET, NULL) == 0

            nbytes = zran.zran_read(&index, buffer, TEST_FILE_SIZE)

            assert nbytes == (TEST_FILE_NELEMS - se) * 8

            assert zran.zran_tell(&index) == TEST_FILE_SIZE

            pybuf = <bytes>(<char *>buffer)[:nbytes]
            data  = np.ndarray(nbytes / 8, np.uint64, pybuf)

            for i, val in enumerate(data, se):
                assert val == i
        
        zran.zran_free(&index) 
