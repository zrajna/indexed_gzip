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
import              random
import              hashlib

import numpy     as np


from libc.stdio cimport (FILE, fdopen)

cimport zran

# 2**32 values, at 2 bytes each, is 8GB
# 2**31 values, at 2 bytes each, is 4GB
# 2**30 values, at 2 bytes each, is 2GB
# 2**29 values, at 2 bytes each, is 1GB
TEST_FILE_SIZE = 2**29
TEST_FILE      = 'testdata.gz'



def setup_module():

    random   .seed(1234567)
    np.random.seed(1234567)

    if not op.exists(TEST_FILE):
        gen_test_data(TEST_FILE)

    random   .seed(1234567)
    np.random.seed(1234567)    

        
def teardown_module():
    pass

        
    
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
                


def test_init_file_modes():

    modes = ['r', 'r+', 'w', 'w+', 'a', 'a+']

    cdef zran.zran_index_t index
    cdef FILE             *cfid
    cdef bytes             bmode
    cdef char             *cmode

    for mode in modes:

        with open(TEST_FILE, mode) as pyfid:

            bmode    = mode.encode()
            cmode    = bmode
            cfid     = fdopen(pyfid.fileno(), cmode)

            expected = mode == 'r'

            print('Opened file with mode=\'{}\' '
                  '(expected: {})'.format(mode, expected))

            result = not zran.zran_init(&index, cfid, 0, 0, 0, 0)

            assert result == expected


def test_seek_and_tell():

    seekpoints = [random.randint(0, TEST_FILE_SIZE) for i in range(100)]

    with open(TEST_FILE, 'rb') as pyfid:
        cfid = fdopen(pyfid.fileno(), 'rb')

        # TODO ...


def gen_test_data(filename):
    """Make some data to test with. """
    
    print('Generating test data')

    data = np.random.randint(0, 65535, TEST_FILE_SIZE)
    data = np.array(data, dtype=np.uint16)
                  
    with gzip.GzipFile(filename, 'wb') as f:
        f.write(data.tostring())


def md5(data):
    
    hashobj = hashlib.md5()
    hashobj.update(data)
    return str(hashobj.hexdigest())
