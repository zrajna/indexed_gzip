#!/usr/bin/env python

from setuptools    import setup
from setuptools    import Extension
from Cython.Build  import cythonize


setup(
    name='indexed_gzip',
    version='0.2',
    author='Paul McCarthy',
    author_email='pauldmccarthy@gmail.com',
    description='Fast random access of gzip files in Python',
    url='https://github.com/pauldmccarthy/indexed_gzip',
    license='zlib',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: zlib/libpng License',
        'Programming Language :: C',
        'Programming Language :: Cython',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: System :: Archiving :: Compression',
    ],
    
    ext_modules=cythonize([
        Extension('indexed_gzip',
                  ['indexed_gzip.pyx', 'zran.c'],
                  libraries=['z'],
                  extra_compile_args=['-Wno-unused-function']),
        
        Extension('tests.ctest_zran',
                  ['tests/ctest_zran.pyx', 'zran.c'],
                  libraries=['z'],
                  include_dirs=['.'],
                  extra_compile_args=['-Wno-unused-function'])
    ]),

    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'numpy'],
    test_suite='tests',
)
