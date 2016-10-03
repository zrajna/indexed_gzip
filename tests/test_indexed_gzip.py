#!/usr/bin/env python
#
# test_indexed_gzip.py - Python wrapper around ctest_indexed_gzip.pyx.
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#


import pytest
pytestmark = pytest.mark.indexed_gzip_test

from . import ctest_indexed_gzip


def setup_module():                 ctest_indexed_gzip.setup_module()
def teardown_module():              ctest_indexed_gzip.teardown_module()

def test_open_close():              ctest_indexed_gzip.test_open_close()
def test_open_close_ctxmanager():   ctest_indexed_gzip.test_open_close_ctxmanager()
def test_create_from_open_handle(): ctest_indexed_gzip.test_create_from_open_handle()
def test_read_all():                ctest_indexed_gzip.test_read_all()
def test_seek_and_read():           ctest_indexed_gzip.test_seek_and_read()
