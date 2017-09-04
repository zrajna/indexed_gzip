#!/usr/bin/env python
#
# __init__.py -
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#

import tempfile
import shutil


def testdir():
    """Returnsa context manager which creates and returns a temporary
    directory, and then deletes it on exit.
    """

    class ctx(object):

        def __enter__(self):

            self.testdir = tempfile.mkdtemp()
            return self.testdir

        def __exit__(self, *a, **kwa):
            shutil.rmtree(self.testdir)

    return ctx()
