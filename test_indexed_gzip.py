#!/usr/bin/env python
#
# test_indexed_gzip.py -
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#

from __future__ import print_function

import                 sys
import                 time
import                 hashlib
import                 random
import                 gzip
import itertools    as it
import indexed_gzip as igzip


def create_gz(fid):
    return gzip.GzipFile(fileobj=fid)


def create_igz(fid):
    return igzip.IndexedGzipFile(fid, init_index=True) 


def md5(data):
    hashobj = hashlib.md5()
    hashobj.update(data)
    return str(hashobj.hexdigest())



def testfile(name, fid, createfunc, length, offsets, loud):

    print('Testing {}...'.format(name))

    fid.seek(0)

    if loud:
        print('  creating file object...')

    createstart = time.time()
    fileobj     = createfunc(fid)
    createend   = time.time()

    seektimes = []
    readtimes = []
    hashtimes = []
    hashes    = []

    for offset in offsets:

        if loud:
            print('  seeking to {}...'.format(offset))

        seekstart = time.time()
        fileobj.seek(offset)
        seekend   = time.time()

        if loud:
            print('  reading {} bytes...'.format(length), end='')

        readstart = time.time()
        data      = fileobj.read(length)
        readend   = time.time()

        hashstart = time.time()
        datahash  = md5(data)
        hashend   = time.time()

        readtimes.append(readend - readstart)
        seektimes.append(seekend - seekstart)
        hashtimes.append(hashend - hashstart)
        hashes   .append(md5(data))

        if loud:
            # startbytes = ''.join(['{:02x}'.format(b) for b in data[:5]])
            # endbytes   = ''.join(['{:02x}'.format(b) for b in data[-5:]])
            # print('{} ... {} [{}]'.format(startbytes, endbytes, len(data)), end=', ')
            print(hashes[-1])

    createtime = createend - createstart
    seektime   = sum(seektimes) / len(seektimes)
    readtime   = sum(readtimes) / len(readtimes)
    hashtime   = sum(hashtimes) / len(hashtimes)
    totaltime  = createtime + sum(seektimes) + sum(readtimes) + sum(hashtimes)
    totalhash  = ''.join(it.chain(*hashes)).encode('ascii')
    datahash   = md5(totalhash)

    print('  done!')
    print('  {} Total time:  {:0.2f} sec'       .format(name, totaltime))
    print('  create time: {:0.2f} sec'          .format(createtime))
    print('  md5 time:    {:0.2f} sec'          .format(hashtime))
    print('  data md5:    {}'                   .format(datahash))
    print('  Average times over {} seeks/reads:'.format(numseeks))
    print('    seek time: {:0.2f} sec'          .format(seektime))
    print('    read time: {:0.2f} sec'          .format(readtime))
    

if len(sys.argv) not in (2, 3, 4, 5):
    print('usage: test_indexed_gzip.py filename [numseeks [loud [seed]]]')
    sys.exit(1)
    
infile = sys.argv[1]

numseeks = 10
loud     = False
seed     = random.randint(1, 2**32)

if len(sys.argv) >= 3: numseeks = int( sys.argv[2])
if len(sys.argv) >= 4: loud     = bool(sys.argv[3])
if len(sys.argv) >= 5: seed     = int( sys.argv[4])


print('Seeding random with {}'.format(seed))
random.seed(seed)


with open(infile, 'rb') as fid:

    fid.seek(-1, 2)
    cmp_size = fid.tell()

    maxoff   = int(cmp_size * 1.5)
    offstep  = int(maxoff / numseeks)
    offsets  = list(range(0, maxoff, offstep))
    length   = int(0.75 * cmp_size / len(offsets))
    numseeks = len(offsets)

    random.shuffle(offsets)
    
    testfile("gzip.GzipFile",         fid, create_gz,  length, offsets, loud)
    testfile("igzip.IndexedGzipFile", fid, create_igz, length, offsets, loud)
