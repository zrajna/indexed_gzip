#!/usr/bin/env python
#
# __init__.py - The indexed_gzip namespace.
#
"""The indexed_gzip namespace. """


from importlib.metadata import version, PackageNotFoundError

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


try:
    __version__ = version("indexed_gzip")
except PackageNotFoundError:
    __version__ = '<unknown>'
