#!/usr/bin/env python
#
# conftest.py -
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#

import os.path as op
import numpy   as np

import pytest

from . import ctest_zran

    
def pytest_addoption(parser):
    
    parser.addoption('--nelems',
                     type=int,
                     action='store',
                     default=2**24 + 1,
                     help='Number of uint64 elements for test data')
    
    parser.addoption('--concat',
                     action='store_true',
                     help='Generate test data made of '
                          'concatenated GZIP streams')
    
    parser.addoption('--use_mmap',
                     action='store_true',
                     help='Use mmap for read buffer instead of main memory')

    parser.addoption('--seed',
                     type=int,
                     help='Seed for random number generator') 

    parser.addoption('--testfile',
                     action='store',
                     help='Name of test file') 

    parser.addoption('--niters',
                     type=int,
                     action='store',
                     default=5000,
                     help='Number of inputs for tests which '
                          'use a random set of inputs')
    
@pytest.fixture
def nelems(request):
    return request.config.getoption('--nelems')

    
@pytest.fixture
def niters(request):
    return request.config.getoption('--niters')


@pytest.fixture
def concat(request):
    return request.config.getoption('--concat')


@pytest.fixture
def use_mmap(request):
    return request.config.getoption('--use_mmap')

@pytest.fixture
def seed(request):
    
    seed = request.config.getoption('--seed')

    if seed is None:
        seed = np.random.randint(2 ** 32)
        
    np.random.seed(seed)
    print('Seed for random number generator: {}'.format(seed))
    return seed



@pytest.fixture
def testfile(request):

    filename = request.config.getoption('--testfile')
    _nelems  = nelems(request)
    _concat  = concat(request)

    if filename is None:
        filename = 'ctest_zran_{}_{}.gz'.format(_nelems, _concat)

    if not op.exists(filename):
        ctest_zran.gen_test_data(filename, _nelems, _concat)

    return filename

