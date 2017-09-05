#
# The IndexedGzipFile class.
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#

"""This module provides the :class:`IndexedGzipFile` class, a drop-in
replacement for the built-in ``gzip.GzipFile`` class, for faster read-only
random access to gzip files.
"""


from libc.stdio  cimport (SEEK_SET,
                          SEEK_CUR,
                          FILE,
                          fdopen,
                          fclose)

from libc.stdint cimport (uint64_t,
                          int64_t)

from posix.types cimport  off_t

from cpython.mem cimport (PyMem_Malloc,
                          PyMem_Realloc,
                          PyMem_Free)

cimport zran

import threading
import logging


log = logging.getLogger(__name__)
log.setLevel(logging.WARNING)


class NotCoveredError(Exception):
    """Exception raised by the :class:`IndexedGzipFile` when an attempt is
    made to seek to/read from a location that is not covered by the
    index. This exception will never be raised if the ``IndexedGzipFile`` was
    created with ``auto_build=True``.
    """
    pass


class ZranError(Exception):
    """Exception raised by the :class:`IndexedGzipFile` when the ``zran``
    library signals an error.
    """
    pass


cdef class IndexedGzipFile:
    """The ``IndexedGzipFile`` class allows for fast random access of a gzip
    file by using the ``zran`` library to build and maintain an index of seek
    points into the file.

    .. note:: The :meth:`seek` and :meth:`read` methods release the GIL while
              calling ``zran`` functions, but the ``IndexedGzipFile`` is *not*
              thread-safe. Use the ``SafeIndexedGzipFile`` class if you need
              thread-safety.
    """


    cdef zran.zran_index_t index
    """A reference to the ``zran_index`` struct. """


    cdef bint auto_build
    """Flag which is set to ``True`` if the file index is built automatically
    on seeks/reads.
    """

    cdef unsigned int readall_buf_size
    """Used by meth:`read` as the read buffer size."""


    cdef bint own_file
    """Flag which is set to ``True`` if the user specified a file name instead
    of an open file handle. In this case, the IndexedGzipFile is responsible
    for closing the file handle when it is destroyed.
    """


    cdef FILE *cfid
    """A reference to the C file handle. """


    cdef object pyfid
    """A reference to the python file handle. """


    def __cinit__(self,
                  filename=None,
                  fid=None,
                  auto_build=True,
                  spacing=4194304,
                  window_size=32768,
                  readbuf_size=1048576,
                  readall_buf_size=16777216):
        """Create an ``IndexedGzipFile``. The file may be specified either
        with an open file handle, or with a filename. If the former, the file
        must have been opened in ``'rb'`` mode.

        :arg filename:         File name.

        :arg fid:              Open file handle.

        :arg auto_build:       If ``True`` (the default), the index is
                               automatically built on seeks/reads.

        :arg spacing:          Number of bytes between index seek points.

        :arg window_size:      Number of bytes of uncompressed data stored with
                               each seek point.

        :arg readbuf_size:     Size of buffer in bytes for storing compressed
                               data read in from the file.


        :arg readall_buf_size: Size of buffer in bytes used by :meth:`read`
                               when reading until EOF.
        """

        if fid is None and filename is None:
            raise ValueError('One of fid or filename must be specified')

        if fid is not None and fid.mode != 'rb':
            raise ValueError('The gzip file must be opened in '
                             'read-only binary ("rb") mode')

        self.own_file         = fid is None
        self.auto_build       = auto_build
        self.readall_buf_size = readall_buf_size

        if self.own_file: self.pyfid = open(filename, 'rb')
        else:             self.pyfid = fid

        self.cfid = fdopen(self.pyfid.fileno(), 'rb')

        if self.auto_build: flags = zran.ZRAN_AUTO_BUILD
        else:               flags = 0

        if zran.zran_init(index=&self.index,
                          fd=self.cfid,
                          spacing=spacing,
                          window_size=window_size,
                          readbuf_size=readbuf_size,
                          flags=flags):
            raise ZranError('zran_init returned error')

        log.debug('{}.__init__({}, {}, {}, {}, {}, {})'.format(
            type(self).__name__,
            fid,
            filename,
            auto_build,
            spacing,
            window_size,
            readbuf_size))


    def __init__(self, *args, **kwargs):
        """This method does nothing. It is here to make sub-classing
        ``IndexedGzipFile`` easier.
        """
        pass


    def fileno(self):
        """Calls ``fileno`` on the underlying file object. """
        return self.pyfid.fileno()


    def close(self):
        """Closes this ``IndexedGzipFile``. """

        if self.closed:
            raise IOError('IndexedGzipFile is already closed')

        zran.zran_free(&self.index)

        if self.own_file:
            self.pyfid.close()

        self.cfid  = NULL
        self.pyfid = None

        log.debug('{}.close()'.format(type(self).__name__))


    @property
    def closed(self):
        """Returns ``True`` if this ``IndexedGzipFile`` is closed, ``False``
        otherwise.
        """
        return self.pyfid is None


    def readable(self):
        """Returns ``True`` if this ``IndexedGzipFile`` is readable, ``False``
        otherwise.
        """
        return not self.closed


    def writable(self):
        """Currently always returns ``False`` - the ``IndexedGzipFile`` does
        not support writing yet.
        """
        return False


    def seekable(self):
        """Returns ``True`` if this ``IndexedGzipFile`` supports seeking,
        ``False`` otherwise.
        """
        return not self.closed


    def tell(self):
        """Returns the current seek offset into the uncompressed data stream.
        """
        return zran.zran_tell(&self.index)


    def __enter__(self):
        """Returns this ``IndexedGzipFile``. """
        return self


    def __exit__(self, *args):
        """Calls close on this ``IndexedGzipFile``. """
        if not self.closed:
            self.close()


    def __dealloc__(self):
        """Frees the memory used by this ``IndexedGzipFile``. If a file name
        was passed to :meth:`__cinit__`, the file handle is closed.
        """
        if not self.closed:
            self.close()


    def build_full_index(self):
        """Re-builds the full file index. """

        if zran.zran_build_index(&self.index, 0, 0) != 0:
            raise ZranError('zran_build_index returned error')

        log.debug('{}.build_fuill_index()'.format(type(self).__name__))


    def seek(self, offset, whence=SEEK_SET):
        """Seeks to the specified position in the uncompressed data stream.

        If this ``IndexedGzipFile`` was created with ``auto_build=False``,
        and the requested offset is not covered by the index, a
        :exc:`NotCoveredError` is raised.

        If ``whence`` is not one of ``SEEK_SET`` or ``SEEK_CUR``, a
        :exc:`ValueError` is raised.

        .. note:: This method releases the GIL while ``zran_seek`` is
                  running.
        """

        cdef int                ret
        cdef off_t              off      = offset
        cdef int                c_whence = whence
        cdef zran.zran_index_t *index    = &self.index

        if whence not in (SEEK_SET, SEEK_CUR):
            raise ValueError('Seek from end not supported')

        with nogil:
            ret = zran.zran_seek(index, off, c_whence, NULL)

        if ret < 0:
            raise ZranError('zran_seek returned error')

        elif ret > 0:
            raise NotCoveredError('Index does not cover '
                                  'offset {}'.format(offset))

        log.debug('{}.seek({})'.format(type(self).__name__, offset))


    def read(self, nbytes=-1):
        """Reads up to ``nbytes`` bytes from the uncompressed data stream.
        If ``nbytes < 0`` the stream is read until EOF.

        .. note:: This method releases the GIL while ``zran_read`` is
                  running.
        """

        if   nbytes == 0: return bytes()
        elif nbytes <  0: buf = ReadBuffer(self.readall_buf_size)
        else:             buf = ReadBuffer(nbytes)

        cdef zran.zran_index_t *index  = &self.index
        cdef size_t             nread  = 0
        cdef uint64_t           bufsz  = buf.size
        cdef size_t             offset = 0
        cdef void              *buffer
        cdef int64_t            ret

        # Read until EOF or enough
        # bytes have been read
        while True:

            buffer = buf.buffer + offset

            # read some bytes
            with nogil:
                ret = zran.zran_read(index, buffer, bufsz)

            # see how the read went
            if ret == zran.ZRAN_READ_FAIL:
                raise ZranError('zran_read returned error ({})'.format(ret))

            # This will happen if the current
            # seek point is not covered by the
            # index, and auto-build is disabled
            elif ret == zran.ZRAN_READ_NOT_COVERED:
                raise NotCoveredError('Index does not cover current offset')

            # No bytes were read, and there are
            # no more bytes to read. This will
            # happen when the seek point was at
            # or beyond EOF when zran_read was
            # called
            elif ret == zran.ZRAN_READ_EOF:
                break

            nread  += ret
            offset += ret

            # If we requested a specific number of
            # bytes, zran_read will have returned
            # them all (or all until EOF), so we're
            # finished
            if nbytes > 0:
                break

            # Otherwise if reading until EOF, check
            # and increase the buffer size if necessary
            if (nread + self.readall_buf_size) > buf.size:
                buf.resize(buf.size + self.readall_buf_size)
                offset = nread

        buf.resize(nread)
        pybuf = <bytes>(<char *>buf.buffer)[:nread]

        log.debug('{}.read({})'.format(type(self).__name__, len(pybuf)))

        return pybuf


    def pread(self, nbytes, offset):
        """Seeks to the specified ``offset``, then reads and returns
        ``nbytes``. See :meth:`seek` and :meth:`read`.
        """
        IndexedGzipFile.seek(self, offset)
        return IndexedGzipFile.read(self, nbytes)


    def write(self, *args, **kwargs):
        """Currently raises a :exc:`NotImplementedError`."""
        raise NotImplementedError('IndexedGzipFile does not support writing')


cdef class ReadBuffer:
    """Wrapper around a chunk of memory.

    .. see:: http://docs.cython.org/src/tutorial/memory_allocation.html
    """

    cdef void *buffer
    """A raw chunk of bytes. """


    cdef size_t size;
    """Size of the buffer. """


    def __cinit__(self, size_t size):
        """Allocate ``size`` bytes of memory. """

        self.size   = size
        self.buffer = PyMem_Malloc(size)

        if not self.buffer:
            raise MemoryError('PyMem_Malloc fail')

        log.debug('ReadBuffer.__cinit__({})'.format(size))


    def resize(self, size_t size):
        """Re-allocate the memory to the given ``size``. """

        if size == self.size:
            return

        buf = PyMem_Realloc(self.buffer, size)

        if not buf:
            raise MemoryError('PyMem_Realloc fail')

        log.debug('ReadBuffer.resize({})'.format(size))

        self.size   = size
        self.buffer = buf


    def __dealloc__(self):
        """Free the mwmory. """
        PyMem_Free(self.buffer)

        log.debug('ReadBuffer.__dealloc__()')



class SafeIndexedGzipFile(IndexedGzipFile):
    """The ``SafeIndexedGzipFile`` is a sub-class of the
    :class:`IndexedGzipFile` which provides (limited) thread-safe access to a
    file. Access to the :meth:`seek`, :meth:`read`, and :meth:`write` methods
    is mutually exclusive, i.e. only a single thread may accesss them at any
    time.
    """


    def __mutex(func):
        """Decorator which marks a method as being mutually exclusive. Access
        to the method is protected by a ``threading.Lock`` object.
        """

        def decorator(self, *args, **kwargs):

            self.__fileLock.acquire()

            try:
                return func(self, *args, **kwargs)

            finally:
                self.__fileLock.release()

        return decorator


    def __init__(self, *args, **kwargs):
        """Creates a ``Lock`` for protecting access to the file methods. """
        IndexedGzipFile.__init__(self, *args, **kwargs)
        self.__fileLock = threading.Lock()


    @__mutex
    def seek(self, *args, **kwargs):
        """See :meth:`IndexedGzipFile.seek`. """
        return IndexedGzipFile.seek(self, *args, **kwargs)


    @__mutex
    def read(self, *args, **kwargs):
        """See :meth:`IndexedGzipFile.read`. """
        return IndexedGzipFile.read(self, *args, **kwargs)


    @__mutex
    def pread(self, *args, **kwargs):
        """See :meth:`IndexedGzipFile.pread`. """
        return IndexedGzipFile.pread(self, *args, **kwargs)


    @__mutex
    def write(self, *args, **kwargs):
        """See :meth:`IndexedGzipFile.write`. """
        return IndexedGzipFile.write(self, *args, **kwargs)
