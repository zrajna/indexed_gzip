#!/usr/bin/env python
#
# test_indexed_gzip.py - Python wrapper around ctest_indexed_gzip.pyx.
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#


import pytest

from . import ctest_indexed_gzip


pytestmark = pytest.mark.indexed_gzip_test


def test_open_close(             testfile, nelems, seed):         ctest_indexed_gzip.test_open_close(             testfile, nelems, seed)
def test_open_close_ctxmanager(  testfile, nelems, seed):         ctest_indexed_gzip.test_open_close_ctxmanager(  testfile, nelems, seed)
def test_atts(                   testfile):                       ctest_indexed_gzip.test_atts(                   testfile)
def test_init_failure_cases(     concat):                         ctest_indexed_gzip.test_init_failure_cases(     concat)
def test_init_success_cases(     concat):                         ctest_indexed_gzip.test_init_success_cases(     concat)
def test_create_from_open_handle(testfile, nelems, seed):         ctest_indexed_gzip.test_create_from_open_handle(testfile, nelems, seed)
def test_read_all(               testfile, nelems, use_mmap):     ctest_indexed_gzip.test_read_all(               testfile, nelems, use_mmap)
def test_read_beyond_end(        concat):                         ctest_indexed_gzip.test_read_beyond_end(        concat)
def test_seek_and_read(          testfile, nelems, niters, seed): ctest_indexed_gzip.test_seek_and_read(          testfile, nelems, niters, seed)
def test_seek_and_tell(          testfile, nelems, niters, seed): ctest_indexed_gzip.test_seek_and_tell(          testfile, nelems, niters, seed)
def test_readline():                                              ctest_indexed_gzip.test_readline()
def test_readline_sizelimit():                                    ctest_indexed_gzip.test_readline_sizelimit()
def test_readlines():                                             ctest_indexed_gzip.test_readlines()
def test_readlines_sizelimit():                                   ctest_indexed_gzip.test_readlines_sizelimit()
def test_iter():                                                  ctest_indexed_gzip.test_iter()
