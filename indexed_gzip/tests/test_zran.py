#!/usr/bin/env python
#
# test_zran.py - Python wrapper around ctest_zran.pyx.
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#

from __future__ import print_function

import sys

if not sys.platform.startswith("win"):
    # Run these tests only on POSIX systems
    import pytest

    import numpy as np

    from . import ctest_zran


    pytestmark = pytest.mark.zran_test

    def test_file_funcs(testfile):
        ctest_zran.test_file_funcs(testfile)

    def test_init(testfile):
        for no_fds in (True, False):
            ctest_zran.test_init(testfile, no_fds)

    def test_init_file_modes(testfile):
        for no_fds in (True, False):
            ctest_zran.test_init_file_modes(testfile, no_fds)

    def test_no_auto_build(testfile, nelems):
        for no_fds in (True, False):
            ctest_zran.test_no_auto_build(testfile, no_fds, nelems)

    def test_seek_to_end(testfile, nelems):
        for no_fds in (True, False):
            ctest_zran.test_seek_to_end(testfile, no_fds, nelems)

    def test_seek_cur(testfile, nelems):
        for no_fds in (True, False):
            ctest_zran.test_seek_cur(testfile, no_fds, nelems)

    def test_seek_end(testfile, nelems):
        for no_fds in (True, False):
            ctest_zran.test_seek_end(testfile, no_fds, nelems)

    def test_seek_beyond_end(testfile, nelems):
        for no_fds in (True, False):
            ctest_zran.test_seek_beyond_end(testfile, no_fds, nelems)

    def test_sequential_seek_to_end(testfile, nelems, niters):
        for no_fds in (True, False):    
            ctest_zran.test_sequential_seek_to_end(testfile, no_fds, nelems, niters)

    def test_random_seek(testfile, nelems, niters, seed):
        for no_fds in (True, False):    
            ctest_zran.test_random_seek(testfile, no_fds, nelems, niters, seed)

    def test_read_all(testfile, nelems, use_mmap):
        for no_fds in (True, False):    
            ctest_zran.test_read_all(testfile, no_fds, nelems, use_mmap)

    @pytest.mark.slow_test
    def test_seek_then_read_block(testfile, nelems, niters, seed, use_mmap):
        for no_fds in (True, False):
            ctest_zran.test_seek_then_read_block(
                testfile, no_fds, nelems, niters, seed, use_mmap
            )

    def test_random_seek_and_read(testfile, nelems, niters, seed):
        for no_fds in (True, False):
            ctest_zran.test_random_seek_and_read(testfile, no_fds, nelems, niters, seed)

    @pytest.mark.slow_test
    def test_read_all_sequential(testfile, nelems):
        for no_fds in (True, False):
            ctest_zran.test_read_all_sequential(testfile, no_fds, nelems)

    @pytest.mark.slow_test
    def test_build_then_read(testfile, nelems, seed, use_mmap):
        for no_fds in (True, False):
            ctest_zran.test_build_then_read(testfile, no_fds, nelems, seed, use_mmap)

    @pytest.mark.slow_test
    def test_readbuf_spacing_sizes(testfile, nelems, niters, seed):
        for no_fds in (True, False):
            ctest_zran.test_readbuf_spacing_sizes(
                testfile, no_fds, nelems, niters, seed
            )

    def test_export_then_import(testfile):
        for no_fds in (True, False):
            ctest_zran.test_export_then_import(testfile, no_fds)

    def test_export_import_no_points():
        for no_fds in (True, False):
            ctest_zran.test_export_import_no_points(no_fds)
