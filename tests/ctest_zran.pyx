#
# Tests for the zran module.
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#

from __future__ import print_function
from __future__ import division

import                    os
import os.path         as op
import itertools       as it
import subprocess      as sp
import multiprocessing as mp
import                    sys
import                    time
import                    random
import                    hashlib
import                    tempfile

import numpy as np

cimport numpy as np

from posix.types cimport  off_t

from libc.stdio  cimport (SEEK_SET,
                          FILE,
                          fdopen,
                          fwrite)

from libc.string cimport memset

from cpython.mem cimport (PyMem_Malloc,
                          PyMem_Realloc,
                          PyMem_Free)

from posix.mman cimport (mmap,
                         munmap,
                         PROT_READ,
                         PROT_WRITE,
                         MAP_ANON,
                         MAP_SHARED)

cdef extern from "sys/mman.h":
    cdef enum:
        MAP_FAILED

cimport zran

np.import_array()


def gen_test_data(filename, nelems, concat):
    """Make some data to test with. """
    
    start = time.time()

    # The file just contains a sequentially
    # increasing list of numbers

    print('Generating test data ({} elems, {} bytes -> {})'.format(
        nelems,
        nelems * 8,
        filename))

    # Generate the data as a numpy memmap array.
    # Allocate at most 128MB at a time
    toWrite        = nelems
    offset         = 0
    writeBlockSize = min(16777216, nelems)

    tmpfile = '{}_temp'.format(filename)

    with open(tmpfile, 'wb+') as f:
        data = np.memmap(tmpfile, dtype=np.uint64, shape=nelems)

        idx = 0

        while toWrite > 0:

            if idx % 10 == 0:
                print('Generated to {}...'.format(offset))

            thisWrite = min(writeBlockSize, toWrite)

            vals = np.arange(offset, offset + thisWrite, dtype=np.uint64)
        
            data[offset:offset + thisWrite] = vals

            toWrite  -= thisWrite
            offset   += thisWrite
            idx      += 1
            data.flush()

    # Not using the python gzip module, 
    # because it is super-slow.

    # Now write that array to a compresed file
    # If a single stream, we just pass the array
    # directly to gzip.
    if not concat:

        print('Compressing all data with a single gzip call ...')

        with open(tmpfile,  'rb') as inf, open(filename, 'wb') as outf:
            sp.call(['gzip', '-c'], stdin=inf, stdout=outf)

    # Otherwise, pass in chunks of data to 
    # generate a concatenated gzip stream
    else:
        
        # maxBufSize is in elements, not in bytes
        if not concat: maxBufSize = nelems
        else:          maxBufSize = min(16777216, nelems // 50)

        toWrite = nelems
        index   = 0

        with open(filename, 'wb') as f:

            idx = 0

            while toWrite > 0:

                if idx % 10 == 0:
                    print('Compressed to {}...'.format(index))

                nvals    = min(maxBufSize, toWrite)
                toWrite -= nvals

                vals     = data[index : index + nvals]

                vals     = vals.tostring()
                index   += nvals

                proc = sp.Popen(['gzip', '-c'], stdin=sp.PIPE, stdout=f)
                proc.communicate(input=vals)

                idx += 1

    end = time.time()

    os.remove(tmpfile)

    print('Done in {:0.2f} seconds'.format(end - start))


cdef read_element(zran.zran_index_t *index, element, nelems, seek=True):

    cdef void *buffer

    buf    = ReadBuffer(8)
    buffer = buf.buffer

    if   element >= nelems: expseek = zran.ZRAN_SEEK_EOF
    else:                   expseek = zran.ZRAN_SEEK_OK

    if   element >= nelems: exptell = (nelems * 8)
    else:                   exptell = element * 8


    if seek:
        gotseek = zran.zran_seek(index, element * 8, SEEK_SET, NULL)
        gottell = zran.zran_tell(index)
        try:
            assert gotseek == expseek
            assert gottell == exptell
        except:
            print('expseek: {}'.format(expseek))
            print('exptell: {}'.format(exptell))
            print('gotseek: {}'.format(gotseek))
            print('gottell: {}'.format(gottell))
            raise
            

    if   element >= nelems: expread = zran.ZRAN_READ_EOF
    else:                   expread = 8
    
    if   element >= nelems: exptell = (nelems * 8)
    else:                   exptell = (element + 1) * 8

    gotread = zran.zran_read(index, buffer, 8)
    gottell = zran.zran_tell(index)

    try:
        assert gotread == expread
        assert gottell == exptell
    except:
        print('nelems:  {}'.format(nelems))
        print('element: {}'.format(element))
        print('expread: {}'.format(expread))
        print('exptell: {}'.format(exptell))
        print('gotread: {}'.format(gotread))
        print('gottell: {}'.format(gottell))
        raise

    if element < nelems:
        pybuf = <bytes>(<char *>buffer)[:8]
        val   = np.ndarray(1, np.uint64, buffer=pybuf)
        return val[0]
    else:
        return None


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

    chunksize = 10000000

    pool = mp.Pool()

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

    cdef bint use_mmap
    """
    """

    cdef size_t size
    """
    """

    cdef object mmap_fd
    cdef object mmap_path
    

    def __cinit__(self, size_t size, use_mmap=False):
        """Allocate ``size`` bytes of memory. """

        self.use_mmap  = use_mmap
        self.mmap_fd   = None
        self.mmap_path = None
        self.size      = size
        self.buffer    = NULL

        if not self.use_mmap:
            self.buffer = PyMem_Malloc(size)
            
            memset(self.buffer, 0, size);
 
        else:

            fd, path = tempfile.mkstemp('readbuf_mmap_{}'.format(id(self)))

            print('Memory-mapping {:0.2f} GB ({})'.format(size / 1073741824., path))

            towrite = size

            while towrite > 0:

                zeros    = np.zeros(min(towrite, 134217728), dtype=np.uint8)
                towrite -= len(zeros)

                os.write(fd, zeros.tostring())

            self.mmap_fd   = fd
            self.mmap_path = path
            self.buffer    = mmap(NULL,
                                  size,
                                  PROT_READ | PROT_WRITE,
                                  MAP_SHARED,
                                  fd,
                                  0)

            if self.buffer == <void*>MAP_FAILED:
                raise RuntimeError('mmap fail')

        if not self.buffer:
            raise RuntimeError('ReadBuffer init fail')


    def resize(self, size_t size):
        """Re-allocate the memory to the given ``size``. """

        if self.use_mmap:
            raise NotImplementedError('Cannot resize a memmapped array!')

        buf = PyMem_Realloc(self.buffer, size)

        if not buf:
            raise MemoryError('PyMem_Realloc fail')

        self.buffer = buf
        self.size   = size


    def __dealloc__(self):
        """Free the mwmory. """

        if not self.use_mmap:
            PyMem_Free(self.buffer)
        else:
            munmap(self.buffer, self.size)
            os.close( self.mmap_fd)
            os.remove(self.mmap_path)

    
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


def test_no_auto_build(testfile, nelems):

    cdef zran.zran_index_t index
    cdef void             *buffer

    filesize     = nelems * 8
    indexSpacing = max(1048576, filesize // 1500)
    bufSize      = 1048576
    buf          = ReadBuffer(bufSize)
    buffer       = buf.buffer 
    
    with open(testfile, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  0)

        assert zran.zran_seek(&index, 0, SEEK_SET, NULL) == zran.ZRAN_SEEK_OK
        assert zran.zran_tell(&index) == 0
        assert zran.zran_seek(&index, 1, SEEK_SET, NULL) == zran.ZRAN_SEEK_NOT_COVERED
        assert zran.zran_tell(&index) == 0

        gotread = zran.zran_read(&index, buffer, bufSize)
        gottell = zran.zran_tell(&index)

        if bufSize > filesize: expread = filesize
        else:                  expread = bufSize

        if bufSize > filesize: exptell = filesize
        else:                  exptell = bufSize

        try:
            assert gotread == expread
            assert gottell == exptell
        except:
            print("expread: {}".format(expread))
            print("gotread: {}".format(gotread))
            print("exptell: {}".format(exptell))
            print("gottell: {}".format(gottell))
            raise

        pybuf = <bytes>(<char *>buffer)[:gotread]
        data  = np.ndarray(gotread // 8, np.uint64, pybuf) 
        
        assert check_data_valid(data, 0)

        if bufSize < filesize:
            assert zran.zran_read(&index, buffer, bufSize) == zran.ZRAN_READ_NOT_COVERED


def test_seek_to_end(testfile, nelems):
    
    cdef zran.zran_index_t index

    filesize = nelems * 8

    seek_point   = filesize - 1
    indexSpacing = max(524288, filesize // 1500)

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
    indexSpacing = max(524288, filesize // 1500)
    seekpoints   = [filesize - 10,
                    filesize - 2,
                    filesize - 1,
                    filesize,
                    filesize + 1,
                    filesize + 2,
                    filesize + 10]

    with open(testfile, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)

        for sp in seekpoints:

            zs = zran.zran_seek(&index, sp, SEEK_SET, NULL)

            if sp >= filesize: expected = zran.ZRAN_SEEK_EOF
            else:              expected = zran.ZRAN_SEEK_OK
 
            try:
                assert zs == expected
 
            except:
                print("{} != {} [sp={}, size={}]".format(zs, expected, sp, filesize))
                raise
        
            zt = zran.zran_tell(&index)

            if sp >= filesize: expected = filesize
            else:              expected = sp 

            try:
                assert zt == expected
                
            except:
                print("{} != {}".format(zt, expected))
                raise

        zran.zran_free(&index) 

        
def test_sequential_seek_to_end(testfile, nelems, niters):
    
    cdef zran.zran_index_t index

    filesize   = nelems * 8

    seek_points = np.linspace(0, filesize, niters, dtype=np.uint64)
    indexSpacing = max(524288, filesize // 2000)

    with open(testfile, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)

        for sp in seek_points:

            if sp >= filesize:
                expseek = zran.ZRAN_SEEK_EOF
                exptell = filesize
            else:
                expseek = zran.ZRAN_SEEK_OK
                exptell = sp
                
            seek = zran.zran_seek(&index, sp, SEEK_SET, NULL)
            tell = zran.zran_tell(&index)

            try:
                assert seek == expseek
                assert tell == exptell
            except:
                print("expseek: {}".format(expseek))
                print("exptell: {}".format(exptell))
                print("seek:    {}".format(seek))
                print("tell:    {}".format(tell))
                raise
 

        zran.zran_free(&index) 


def test_random_seek(testfile, nelems, niters, seed):

    cdef zran.zran_index_t index

    filesize = nelems * 8

    seekpoints   = [random.randint(0, filesize) for i in range(niters)]
    indexSpacing = max(524288, filesize // 1000)

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


def test_read_all(testfile, nelems, use_mmap):

    filesize = nelems * 8
    indexSpacing = max(524288, filesize // 1000)
    
    cdef zran.zran_index_t index
    cdef void             *buffer
    cdef np.npy_intp       nelemsp

    buf    = ReadBuffer(filesize, use_mmap=use_mmap) 
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

    nelemsp = nbytes / 8.
    data    = np.PyArray_SimpleNewFromData(1, &nelemsp,  np.NPY_UINT64, buffer)

    assert check_data_valid(data, 0)
            

def test_seek_then_read_block(testfile, nelems, niters, seed, use_mmap):
    
    filesize  = nelems * 8
    
    indexSpacing = max(524288, filesize // 1000)
    buf          = ReadBuffer(filesize, use_mmap=use_mmap)
    seekelems    = np.linspace(0, nelems - 1, niters, dtype=np.uint64)
    
    np.random.shuffle(seekelems)
    
    cdef zran.zran_index_t index
    cdef void             *buffer = buf.buffer
    cdef np.npy_intp       nelemsp

    with open(testfile, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)

        for i, se in enumerate(seekelems):


            if se == nelems - 1:
                readelems = 1
            else:
                readelems = np.random.randint(1, nelems - se)

            start = time.time()

            print("{} / {}: reading {} elements from {} ... ".format(
                i, len(seekelems), readelems, se), end='')
                
            assert zran.zran_seek(&index, se * 8, SEEK_SET, NULL) == zran.ZRAN_SEEK_OK
                
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

            nelemsp = nbytes / 8.
            data    = np.PyArray_SimpleNewFromData(1, &nelemsp,  np.NPY_UINT64, buffer)

            assert check_data_valid(data, se, se + readelems)

            end = time.time()

            print("{:0.2f} seconds".format(end - start))
                    
        zran.zran_free(&index)


def test_random_seek_and_read(testfile, nelems, niters, seed):

    cdef zran.zran_index_t index

    filesize = nelems * 8

    seekelems    = np.random.randint(0, nelems, niters)
    indexSpacing = max(524288, filesize // 1000)

    with open(testfile, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        assert not zran.zran_init(&index,
                                  cfid,
                                  indexSpacing,
                                  32768,
                                  131072,
                                  zran.ZRAN_AUTO_BUILD)

        for se in seekelems:

            # Should never happen
            if se >= nelems: expval = None
            else:            expval = se
            
            val = read_element(&index, se, nelems, True)

            try:
                assert val == expval
            except:
                print("{} != {}".format(val, se))
                raise

        zran.zran_free(&index) 


def test_read_all_sequential(testfile, nelems):

    cdef zran.zran_index_t index

    filesize = nelems * 8

    indexSpacing = max(524288, filesize // 1000)

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

            val = read_element(&index, se, nelems, True)
            try:
                assert val == se
            except:
                print("{} != {}".format(val, se))
                print("{:x} != {:x}".format(val, se))
                raise

        zran.zran_free(&index)


def test_build_then_read(testfile, nelems, seed, use_mmap):

    filesize = nelems * 8
    
    indexSpacing = max(524288, filesize // 1000)
    buf          = ReadBuffer(filesize, use_mmap)
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
            data  = np.ndarray(nbytes // 8, np.uint64, pybuf)

            for i, val in enumerate(data, se):
                assert val == i
        
        zran.zran_free(&index) 


def test_readbuf_spacing_sizes(testfile, nelems, niters, seed):

    cdef zran.zran_index_t index

    spacings = [262144,  524288,  1048576,
                2097152, 4194304, 8388608]
    bufsizes = [16384,   65536,   131072,  262144,
                524288,  1048575, 1048576, 1048577,
                2097152, 4194304, 8388608]

    seekelems = np.random.randint(0, nelems, niters // 2)
    seekelems = np.concatenate((spacings, bufsizes, seekelems))

    for sbi, (spacing, bufsize) in enumerate(it.product(spacings, bufsizes)):

        with open(testfile, 'rb') as pyfid:

            print('{} / {}: spacing={}, bufsize={} ... '.format(
                sbi,
                len(spacings) * len(bufsizes),
                spacing, bufsize), end='')

            cfid = fdopen(pyfid.fileno(), 'rb')

            assert not zran.zran_init(&index,
                                      cfid,
                                      spacing,
                                      32768,
                                      bufsize,
                                      zran.ZRAN_AUTO_BUILD)

            for i, se in enumerate(seekelems):

                # print('{} / {}: {}'.format(i, len(seekelems), se))

                if se >= nelems: expval = None
                else:            expval = se

                val = read_element(&index, se, nelems, seek=True)

                try: 
                    assert val == expval
                except:
                    print('{} != {}'.format(val, expval))
                    raise

            print()
            zran.zran_free(&index)
