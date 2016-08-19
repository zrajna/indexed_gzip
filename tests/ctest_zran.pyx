#
# Tests for the zran module.
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

from libc.stdio cimport (SEEK_SET, FILE, fdopen)

from cpython.mem cimport (PyMem_Malloc,
                          PyMem_Realloc,
                          PyMem_Free)

cimport zran


# 2**32 values, at 2 bytes each, is 8GB
# 2**31 values, at 2 bytes each, is 4GB
# 2**30 values, at 2 bytes each, is 2GB
# 2**29 values, at 2 bytes each, is 1GB
TEST_FILE_NELEMS = 2**29
TEST_FILE_SIZE   = TEST_FILE_NELEMS * 2
TEST_FILE        = 'testdata.gz'


def setup_module():

    random.seed(1234567)

    if not op.exists(TEST_FILE):
        gen_test_data(TEST_FILE)

    random.seed(1234567)

        
def teardown_module():
    if op.exists(TEST_FILE):
        os.remove(TEST_FILE)


def gen_test_data(filename):
    """Make some data to test with. """
    
    print('Generating test data')

    start = time.time()

    with gzip.GzipFile(filename, 'wb') as f:

        toWrite    = TEST_FILE_NELEMS
        maxBufSize = 1000000

        while toWrite > 0:

            nvals    = min(maxBufSize, toWrite)
            vals     = [random.randint(0, 65535) for  i in range(nvals)]
            toWrite -= nvals
            buf      = struct.pack('{}H'.format(nvals), *vals)
            
            f.write(buf)

    end = time.time()

    print('Done in {:0.2f} seconds'.format(end - start))


def md5(data):
    
    hashobj = hashlib.md5()
    hashobj.update(data)
    return str(hashobj.hexdigest())


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
            
            print('Testing zran_init(spacing={}, window_size={}, '
                  'readbuf_size={}, flags={}) [{} == {} ?]'.format(
                      s, w, r, f, expected, result))
            
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

            print('Opened file with mode=\'{}\' '
                  '(expected: {})'.format(mode, expected))

            result = not zran.zran_init(&index, cfid, 0, 0, 0, 0)

            assert result == expected

            zran.zran_free(&index)


def test_seek_and_tell():

    cdef zran.zran_index_t index

    seekpoints   = [random.randint(0, TEST_FILE_SIZE) for i in range(500)]
    indexSpacing = TEST_FILE_SIZE / 1000

    with open(TEST_FILE, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)

        for sp in seekpoints:
            
            zran.zran_seek(&index, sp, SEEK_SET, NULL)
            zt = zran.zran_tell(&index)
            print('{} == {}?'.format(zt, sp))
            assert zt == sp

        zran.zran_free(&index)


def test_read_all():

    indexSpacing = TEST_FILE_SIZE / 1000
    
    cdef zran.zran_index_t index
    cdef void             *buffer

    with open(TEST_FILE, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        buf = ReadBuffer(TEST_FILE_SIZE)

        buffer = buf.buffer

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)

        nbytes = zran.zran_read(&index, buffer, TEST_FILE_SIZE)

        assert nbytes == TEST_FILE_SIZE
        
        zran.zran_free(&index)


def test_seek_then_read_all():
    
    indexSpacing = TEST_FILE_SIZE / 1000
    buf          = ReadBuffer(TEST_FILE_SIZE)
    seekpoints   = [random.randint(0, TEST_FILE_SIZE) for i in range(500)]
    
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

        for sp in seekpoints:

            zran.zran_seek(&index, sp, SEEK_SET, NULL)

            nbytes = zran.zran_read(&index, buffer, TEST_FILE_SIZE)

            assert nbytes == TEST_FILE_SIZE - sp
        
        zran.zran_free(&index) 


def test_build_then_read():
    
    indexSpacing = TEST_FILE_SIZE / 1000
    buf          = ReadBuffer(TEST_FILE_SIZE)
    seekpoints   = [random.randint(0, TEST_FILE_SIZE) for i in range(500)]
    
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

        for sp in seekpoints:

            zran.zran_seek(&index, sp, SEEK_SET, NULL)

            nbytes = zran.zran_read(&index, buffer, TEST_FILE_SIZE)

            assert nbytes == TEST_FILE_SIZE - sp
        
        zran.zran_free(&index) 
