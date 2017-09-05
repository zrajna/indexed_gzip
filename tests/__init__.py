#!/usr/bin/env python
#
# __init__.py -
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#

import                    os
import                    time
import                    shutil
import                    tempfile
import subprocess      as sp
import multiprocessing as mp

import numpy as np


def testdir():
    """Returnsa context manager which creates and returns a temporary
    directory, and then deletes it on exit.
    """

    class ctx(object):

        def __enter__(self):

            self.testdir = tempfile.mkdtemp()
            return self.testdir

        def __exit__(self, *a, **kwa):
            shutil.rmtree(self.testdir)

    return ctx()


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
            proc = sp.Popen(['gzip', '-c'],
                            stdin=inf,
                            stdout=outf)

            start = time.time()
            while proc.poll() is None:
                time.sleep(0.5)
                cur = time.time()
                elapsed = cur - start
                if int(elapsed) % 60 == 0:
                    print('Waiting for gzip ({:0.2f} minutes)'
                          .format(elapsed / 60.0))


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
