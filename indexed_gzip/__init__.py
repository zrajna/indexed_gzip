#!/usr/bin/env python
#
# __init__.py - The indexed_gzip namespace.
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#
"""The indexed_gzip namespace. """


from .indexed_gzip import (IndexedGzipFile,
                           SafeIndexedGzipFile,
                           DroppingIndexedGzipFile,
                           NotCoveredError,
                           ZranError)


__version__ = '0.6.1'
