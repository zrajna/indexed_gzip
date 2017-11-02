#!/usr/bin/env python
#
# benchmark.py - benchmark indexed_gzip
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#

import os
import sys
import gzip
import time
import shutil
import hashlib
import tempfile
import argparse
import contextlib

import numpy as np

import indexed_gzip as igzip


@contextlib.contextmanager
def tempdir():

    testdir = tempfile.mkdtemp()
    prevdir = os.getcwd()
    try:

        os.chdir(testdir)
        yield testdir

    finally:
        os.chdir(prevdir)
        shutil.rmtree(testdir)


def gen_file(nbytes):

    fname = 'test.gz'
    data  = np.random.randint(0, 255, nbytes, dtype=np.uint8)

    with gzip.open(fname, 'wb') as outf:
        outf.write(data.tostring())

    return fname


def benchmark_file(fobj, seeks, lens, update):

    start   = time.time()
    hashobj = hashlib.md5()

    for i, (s, l) in enumerate(zip(seeks, lens)):
        fobj.seek(s)
        data = fobj.read(l)
        hashobj.update(data)
        update(i)

    update(len(seeks))

    end     = time.time()
    elapsed = end - start

    return str(hashobj.hexdigest()), elapsed


def benchmark(nseeks, nbytes):

    print('Generating test data...')

    filename = gen_file(nbytes)
    seeks    = np.linspace(0, nbytes - 2, nseeks, dtype=np.int)

    np.random.shuffle(seeks)

    lens = [np.random.randint(1, 1 + nbytes - s) for s in seeks]

    names = [
        'GzipFile',
        'IndexedGzipFile(drop_handles=True)',
        'IndexedGzipFile(drop_handles=False)',
        'SafeIndexedGzipFile(drop_handles=True)',
        'SafeIndexedGzipFile(drop_handles=True)',
    ]
    namelen = max([len(n) for n in names])
    namefmt = '{{:<{}s}}'.format(namelen)

    fobjs = [
        lambda : gzip.GzipFile(            filename, 'rb'),
        lambda : igzip.IndexedGzipFile(    filename, drop_handles=True),
        lambda : igzip.IndexedGzipFile(    filename, drop_handles=False),
        lambda : igzip.SafeIndexedGzipFile(filename, drop_handles=True),
        lambda : igzip.SafeIndexedGzipFile(filename, drop_handles=False),
    ]

    for name, fobj in zip(names, fobjs):

        def update(i):
            print('\r{} {:6.2f}%'.format(
                namefmt.format(name),
                100.0 * i / len(seeks)), end='')
            sys.stdout.flush()

        with fobj() as f:
            md5, time = benchmark_file(f, seeks, lens, update)

        print('{} {:0.0f}. {:0.0f}s'.format(md5, (time / 60.0), (time % 60.0)))


if __name__ == '__main__':

    parser = argparse.ArgumentParser('indexe_gzip benchmark')

    parser.add_argument('-b',
                        '--bytes',
                        type=int,
                        help='Size of test file in bytes',
                        default=16777216)
    parser.add_argument('-s',
                        '--seeks',
                        type=int,
                        help='Number of random seeks',
                        default=1000)
    parser.add_argument('-r',
                        '--randomseed',
                        type=int,
                        help='Seed for random number generator')

    namespace = parser.parse_args()

    if namespace.seed is not None:
        np.random.seed(namespace.seed)

    with tempdir():
        benchmark(namespace.seeks, namespace.bytes)
