#
# Tests for the zran module.
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#

from __future__ import print_function

import                    os
import os.path         as op
import itertools       as it
import subprocess      as sp
import multiprocessing as mp
import                    sys
import                    time
import                    random
import                    hashlib

import numpy as np

from libc.stdio cimport (SEEK_SET, FILE, fdopen)

from libc.string cimport memset

from cpython.mem cimport (PyMem_Malloc,
                          PyMem_Realloc,
                          PyMem_Free)

cimport zran


def gen_test_data(filename, nelems, concat):
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


def _check_chunk(args):

    startval, endval, offset, chunksize = args

    s      = startval + offset
    e      = min(s + chunksize, endval)
    nelems = e - s

    valid  = np.arange(s, e, dtype=np.uint64)

    val = np.all(_test_data[offset:offset + nelems] == valid)

    return val


_test_data = None


def check_data_valid(data, startval, endval=None):

    global _test_data
    
    _test_data = data
    data       = None

    if endval is None:
        endval = len(_test_data)

    pool = mp.Pool()

    chunksize = 1000000

    startval = int(startval)
    endval   = int(endval)
    
    offsets = np.arange(0, len(_test_data), chunksize)

    args   = [(startval, endval, off, chunksize) for off in offsets]
    result = all(pool.map(_check_chunk, args))

    pool.terminate()

    return result


cdef class ReadBuffer:
    """Wrapper around a chunk of memory.
 
    .. see:: http://docs.cython.org/src/tutorial/memory_allocation.html
    """

    cdef void *buffer
    """A raw chunk of bytes. """

    
    def __cinit__(self, size_t size):
        """Allocate ``size`` bytes of memory. """

        self.buffer = PyMem_Malloc(size);
        memset(self.buffer, 0, size);
        

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

    
def test_init(testfile):
    """Tests a bunch of permutations of the parameters to zran_init. """

    spacings      = [0, 16384, 32768, 65536, 524288, 1048576, 2097152, 4194304]
    window_sizes  = [0, 8192, 16384, 32768, 65536, 131072]
    readbuf_sizes = [0, 8192, 16384, 24576, 32768, 65536, 131072]
    flags         = [0, zran.ZRAN_AUTO_BUILD]

    cdef zran.zran_index_t index
    cdef FILE             *cfid

    with open(testfile, 'rb') as pyfid:
        
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


def test_init_file_modes(testfile):

    modes = ['r', 'r+', 'w', 'w+', 'a', 'a+']

    files = [testfile, testfile,
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


def test_seek_to_end(testfile, nelems):
    
    cdef zran.zran_index_t index

    filesize = nelems * 8

    seek_point   = filesize - 1
    indexSpacing = max(524288, filesize / 1500)

    with open(testfile, 'rb') as pyfid:
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


def test_seek_beyond_end(testfile, nelems):
    
    cdef zran.zran_index_t index

    filesize     = nelems * 8

    seek_point   = filesize + 10
    indexSpacing = max(524288, filesize / 1500)

    with open(testfile, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)
            
        assert zran.zran_seek(&index, seek_point, SEEK_SET, NULL) == 0
        
        zt = zran.zran_tell(&index)

        assert zt == filesize

        zran.zran_free(&index) 

        
def test_sequential_seek_to_end(testfile, nelems, niters):
    
    cdef zran.zran_index_t index

    filesize   = nelems * 8

    seek_points = np.linspace(0, filesize, niters, dtype=np.uint64)
    indexSpacing = max(524288, filesize / 2000)

    with open(testfile, 'rb') as pyfid:
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


def test_random_seek(testfile, nelems, niters):

    cdef zran.zran_index_t index

    filesize = nelems * 8

    seekpoints   = [random.randint(0, filesize) for i in range(niters)]
    indexSpacing = max(524288, filesize / 1000)

    with open(testfile, 'rb') as pyfid:
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


def test_read_all(testfile, nelems):

    filesize = nelems * 8
    indexSpacing = max(524288, filesize / 1000)
    
    cdef zran.zran_index_t index
    cdef void             *buffer

    buf    = ReadBuffer(filesize) 
    buffer = buf.buffer

    with open(testfile, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)

        nbytes = zran.zran_read(&index, buffer, filesize)

        assert nbytes                 == filesize
        assert zran.zran_tell(&index) == nbytes
        
        zran.zran_free(&index)

    pybuf = <bytes>(<char *>buffer)[:nbytes]
    data  = np.ndarray(nelems, np.uint64, pybuf)

    assert check_data_valid(data, 0)
            

def test_seek_then_read_block(testfile, nelems, niters):
    
    filesize  = nelems * 8
    
    indexSpacing = max(524288, filesize / 1000)
    buf          = ReadBuffer(filesize)
    seekelems    = np.linspace(0, nelems - 1, niters, dtype=np.uint64)
    
    np.random.shuffle(seekelems)
    
    cdef zran.zran_index_t index
    cdef void             *buffer = buf.buffer

    with open(testfile, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)

        for i, se in enumerate(seekelems):

            zran.zran_seek(&index, se * 8, SEEK_SET, NULL)

            if se == nelems - 1:
                readelems = 1
            else:
                readelems = np.random.randint(1, nelems - se)
                
            nbytes = zran.zran_read(&index, buffer, readelems * 8)

            try:
                assert nbytes                 == readelems * 8
                assert zran.zran_tell(&index) == (se + readelems) * 8
            except:
                print('seekelem:    {}'.format(se))
                print('readelems:   {}'.format(readelems))
                print('nbytes:      {}'.format(nbytes))
                print('  should be: {}'.format(readelems * 8))
                print('ftell:       {}'.format(zran.zran_tell(&index)))
                print('  should be: {}'.format((se + readelems) * 8))
                raise

            pybuf = <bytes>(<char *>buffer)[:nbytes]
            data  = np.ndarray(nbytes / 8, np.uint64, pybuf)

            assert check_data_valid(data, se, se + readelems)
                    
        zran.zran_free(&index)


def test_random_seek_and_read(testfile, nelems, niters):

    cdef zran.zran_index_t index

    filesize = nelems * 8

    seekelems    = [random.randint(0, nelems) for i in range(niters)]
    indexSpacing = max(524288, filesize / 1000)

    with open(testfile, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)

        for se in seekelems:

            val = read_element(&index, se, True)

            try:
                assert val == se
            except:
                print("{} != {} [{:x} != {:x}]".format(val, se, val, se))
                raise

        zran.zran_free(&index) 


def test_read_all_sequential(testfile, nelems):

    cdef zran.zran_index_t index

    filesize = nelems * 8

    indexSpacing = max(524288, filesize / 1000)

    # Takes too long to read all elements
    seekelems = np.linspace(0, nelems - 1, 10000, dtype=np.uint64)

    with open(testfile, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)

        for se in seekelems:

            val = read_element(&index, se, True)
            try:
                assert val == se
            except:
                print("{} != {}".format(val, se))
                print("{:x} != {:x}".format(val, se))
                raise

        zran.zran_free(&index)


def test_build_then_read(testfile, nelems):

    filesize = nelems * 8
    
    indexSpacing = max(524288, filesize / 1000)
    buf          = ReadBuffer(filesize)
    seekelems    = np.linspace(0, nelems - 1, 5000, dtype=np.uint64)
    np.random.shuffle(seekelems) 
    
    cdef zran.zran_index_t index
    cdef void             *buffer = buf.buffer

    with open(testfile, 'rb') as pyfid:
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

            if se == nelems - 1:
                readelems = 1
            else:
                readelems = np.random.randint(1, min(nelems - se, 5000)) 
                
            nbytes = zran.zran_read(&index, buffer, readelems * 8)

            assert nbytes                 == readelems * 8
            assert zran.zran_tell(&index) == (se + readelems) * 8 

            pybuf = <bytes>(<char *>buffer)[:nbytes]
            data  = np.ndarray(nbytes / 8, np.uint64, pybuf)

            for i, val in enumerate(data, se):
                assert val == i
        
        zran.zran_free(&index) 


def test_readbuf_spacing_sizes(concat, niters):

    test_file        = 'ctest_zran_spacing_testdata.gz'
    test_file_nelems = 2**24 + 1
    test_file_size   = test_file_nelems * 8

    cdef zran.zran_index_t index

    if not op.exists(test_file):
        gen_test_data(test_file, test_file_nelems, concat)

    spacings = [262144,  524288,  1048576,
                2097152, 4194304, 8388608]
    bufsizes = [16384,   65536,   131072,  262144,
                524288,  1048575, 1048576, 1048577,
                2097152, 4194304, 8388608]

    seekelems = np.random.randint(0, test_file_nelems, niters / 2)
    seekelems = np.concatenate((spacings, bufsizes, seekelems))

    for i, (spacing, bufsize) in enumerate(it.product(spacings, bufsizes)):

        with open(test_file, 'rb') as pyfid:

            cfid = fdopen(pyfid.fileno(), 'rb')

            assert not zran.zran_init(&index,
                                      cfid,
                                      spacing,
                                      32768,
                                      bufsize,
                                      zran.ZRAN_AUTO_BUILD)

            for i, se in enumerate(seekelems):

                val = read_element(&index, se, seek=True)

                assert val == se

            zran.zran_free(&index)


    if op.exists(test_file):
        os.remove(test_file)
