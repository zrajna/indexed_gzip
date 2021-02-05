#!/usr/bin/env python
#
# __init__.py - The indexed_gzip namespace.
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#
"""The indexed_gzip namespace. """


from .indexed_gzip import (_IndexedGzipFile,     # noqa
                           IndexedGzipFile,
                           open,
                           NotCoveredError,
                           NoHandleError,
                           ZranError)


SafeIndexedGzipFile = IndexedGzipFile
"""Alias for ``IndexedGzipFile``, to preserve compatibility with older
versions of ``nibabel``.
"""


__version__ = '1.4.1'
