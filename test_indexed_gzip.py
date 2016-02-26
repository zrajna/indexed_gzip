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

    title = 'Testing {}'.format(name)
    print('\n\n{}\n{}\n\n'.format(title,
                                  '=' * len(title)))

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

    for i, offset in enumerate(offsets):

        if loud:
            print('{: 3d}: seeking to {}...'.format(i, offset))
        else:
            print('\r{: 3d}: reading {:0.2f}MB from location {}...'.format(
                i, length / 1048576, offset), end='')

        seekstart = time.time()
        fileobj.seek(offset)
        seekend   = time.time()

        if loud:
            print('     reading {:0.2f}MB...'.format(length / 1048576), end='')

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
            print('data hash: {}\n'.format(hashes[-1]))

    totalhashstart = time.time()
    totalhash      = ''.join(it.chain(*hashes)).encode('ascii')
    datahash       = md5(totalhash)
    totalhashend   = time.time()
 
    createtime = createend - createstart
    seektime   = sum(seektimes)
    readtime   = sum(readtimes)
    hashtime   = sum(hashtimes) + (totalhashend - totalhashstart)
    totaltime  = createtime + seektime + readtime + hashtime

    print('  done!\n\n')

    subtitle = '{} summary'.format(name)
    print('{}\n{}\n'.format(subtitle, '-' * len(subtitle)))
          
    print('Total time:  {:0.2f} sec'          .format(totaltime))
    print('Create time: {:0.2f} sec'          .format(createtime))
    print('Seek time:   {:0.2f} sec'          .format(seektime))
    print('Read time:   {:0.2f} sec'          .format(readtime))
    print('MD5 time:    {:0.2f} sec'          .format(hashtime))
    print('Data MD5:    {}'                   .format(datahash))
    print()
    print('Average times over {} seeks/reads:'.format(numseeks))
    print('  seek time: {:0.2f} sec'          .format(seektime / numseeks))
    print('  read time: {:0.2f} sec'          .format(readtime / numseeks))
    

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


print('Random seed: {}'.format(seed))
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
