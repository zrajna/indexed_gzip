#!/usr/bin/env python
#
# __init__.py - The indexed_gzip namespace.
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#
"""The indexed_gzip namespace. """


from .indexed_gzip import (_IndexedGzipFile,     # noqa
                           IndexedGzipFile,
                           NotCoveredError,
                           NoHandleError,
                           ZranError)


__version__ = '1.3.0'
