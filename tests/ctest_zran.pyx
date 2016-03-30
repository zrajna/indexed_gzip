#
# Tests for the zran module.
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#

from __future__ import print_function

import              os
import os.path   as op
import              sys
import              gzip
import              random
import numpy     as np
import itertools as it

from libc.stdio cimport (FILE, fdopen)

cimport zran


TEST_FILE = 'testdata.gz'


def setup_module():
    if not op.exists(TEST_FILE):
        gen_test_data(TEST_FILE)

        
def teardown_module():
    pass


def gen_test_data(filename):
    """Make some data to test with. """
    
    random.seed(1234567)

    print('Generating test data')

    # 2**32 values, at 2 bytes each, is 8GB
    # 2**31 values, at 2 bytes each, is 4GB
    # 2**30 values, at 2 bytes each, is 2GB
    # 2**29 values, at 2 bytes each, is 1GB
    data = np.array(np.random.randint(0, 65535, 2**29), dtype=np.uint16)
    with gzip.GzipFile(filename, 'wb') as f:
        f.write(data.tostring())
        
    
def test_init():
    """Tests a bunch of permutations of the parameters to zran_init. """

    spacings      = [16384, 32768, 65536, 524288, 1048576, 2097152, 4194304]
    window_sizes  = [8192, 16384, 32768, 65536, 131072]
    readbuf_sizes = [8192, 16384, 24576, 32768, 65536, 131072]
    flags         = [0, zran.ZRAN_AUTO_BUILD]

    cdef zran.zran_index_t index
    cdef FILE             *cfid

    with open(TEST_FILE, 'rb') as pyfid:
        
        cfid = fdopen(pyfid.fileno(), 'rb')

        for s, w, r, f in it.product(spacings,
                                     window_sizes,
                                     readbuf_sizes,
                                     flags):

            expected = (r >= 32768) and (s > w)

            result = not zran.zran_init(&index, cfid, s, w, r, f)
            
            print('Testing zran_init(spacing={}, window_size={}, '
                  'readbuf_size={}, flags={}) [{} == {} ?]'.format(
                      s, w, r, f, expected, result))
            
            assert result == expected
