#
# The IndexedGzipFile class.
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#

"""This module provides the :class:`IndexedGzipFile` class, a drop-in
replacement for the built-in ``gzip.GzipFile`` class, for faster read-only
random access to gzip files.
"""


from libc.stdio     cimport (SEEK_SET,
                             SEEK_CUR,
                             FILE,
                             fdopen,
                             fclose)

from libc.stdint    cimport (uint8_t,
                             uint64_t,
                             int64_t)

from cpython.mem    cimport (PyMem_Malloc,
                             PyMem_Realloc,
                             PyMem_Free)

from cpython.buffer cimport (PyObject_GetBuffer,
                             PyBuffer_Release,
                             PyBUF_ANY_CONTIGUOUS,
                             PyBUF_SIMPLE)

cimport indexed_gzip.zran as zran

import io
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


    cdef object pyfid
    """A reference to the python file handle. """


    cdef object filename
    """String containing path of file being indexed. Used to release and
    reopen file handles between seeks and reads.
    Set to ``None`` if file handle is passed.
    """

    cdef bint finalized
    """Flag which is set to ``True`` if the IndexedGzipFile has been closed.
    Further operations will fail if ``True``.
    """


    def __cinit__(self,
                  filename=None,
                  fid=None,
                  mode=None,
                  auto_build=True,
                  spacing=4194304,
                  window_size=32768,
                  readbuf_size=1048576,
                  readall_buf_size=16777216):
        """Create an ``IndexedGzipFile``. The file may be specified either
        with an open file handle, or with a filename. If the former, the file
        must have been opened in ``'rb'`` mode.

        :arg filename:         File name.

        :arg mode:             Opening mode. Must be either ``'r'`` or ``'rb``.

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

        if (fid is None) and (mode not in (None, 'r', 'rb')):
            raise ValueError('Invalid mode ({}), must be '
                             '\'r\' or \'rb\''.format(mode))
        mode = 'rb'

        self.own_file         = fid is None
        self.auto_build       = auto_build
        self.readall_buf_size = readall_buf_size

        if self.own_file:
            self.filename = filename
            self.pyfid = open(filename, 'rb')
        else:
            self.pyfid = fid

        cfid = fdopen(self.pyfid.fileno(), 'rb')

        if self.auto_build: flags = zran.ZRAN_AUTO_BUILD
        else:               flags = 0

        if zran.zran_init(index=&self.index,
                          fd=cfid,
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

        self._rel_fh()

    def _acq_fh(self):
        if self.own_file:
            self.pyfid = open(self.filename, 'rb')
            self.index.fd = fdopen(self.pyfid.fileno(), 'rb')

    def _rel_fh(self):
        if self.own_file:
            self.pyfid.close()
            self.pyfid = None
            self.index.fd  = NULL

    def __init__(self, *args, **kwargs):
        """This method does nothing. It is here to make sub-classing
        ``IndexedGzipFile`` easier.
        """
        pass


    def fileno(self):
        """Calls ``fileno`` on the underlying file object. """
        if self.own_file:
            raise NotImplementedError
        return self.pyfid.fileno()


    def fileobj(self):
        """Returns a reference to the python file object. """
        if self.own_file:
            raise NotImplementedError
        return self.pyfid


    @property
    def mode(self):
        """Returns the mode that this file was opened in. Currently always
        returns ``'rb'``.
        """
        return 'rb'


    def close(self):
        """Closes this ``IndexedGzipFile``. """

        if self.closed:
            raise IOError('IndexedGzipFile is already closed')

        zran.zran_free(&self.index)

        self.filename = None
        self.pyfid = None
        self.finalized = True

        log.debug('{}.close()'.format(type(self).__name__))


    @property
    def closed(self):
        """Returns ``True`` if this ``IndexedGzipFile`` is closed, ``False``
        otherwise.
        """
        return self.finalized


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

        self._acq_fh()
        ret = zran.zran_build_index(&self.index, 0, 0)
        self._rel_fh()
        if ret != 0:
            raise ZranError('zran_build_index returned error')

        log.debug('{}.build_fuill_index()'.format(type(self).__name__))


    def seek(self, offset, whence=SEEK_SET):
        """Seeks to the specified position in the uncompressed data stream.

        If this ``IndexedGzipFile`` was created with ``auto_build=False``,
        and the requested offset is not covered by the index, a
        :exc:`NotCoveredError` is raised.

        :arg offset: Desired seek offset into the uncompressed data

        :arg whence: Either  ``SEEK_SET`` or ``SEEK_CUR``. If not one of these,
                     a :exc:`ValueError` is raised.

        :returns:    The final seek location into the uncompressed stream.

        .. note:: This method releases the GIL while ``zran_seek`` is
                  running.
        """

        cdef int                ret
        cdef uint64_t           off      = offset
        cdef uint8_t            c_whence = whence
        cdef zran.zran_index_t *index    = &self.index

        if whence not in (SEEK_SET, SEEK_CUR):
            raise ValueError('Seek from end not supported')

        self._acq_fh()
        with nogil:
            ret = zran.zran_seek(index, off, c_whence, NULL)
        self._rel_fh()

        if ret < 0:
            raise ZranError('zran_seek returned error: {}'.format(ret))

        elif ret == zran.ZRAN_SEEK_NOT_COVERED:
            raise NotCoveredError('Index does not cover '
                                  'offset {}'.format(offset))

        elif ret not in (zran.ZRAN_SEEK_OK, zran.ZRAN_SEEK_EOF):
            raise ZranError('zran_seek returned unknown code: {}'.format(ret))

        offset = self.tell()

        log.debug('{}.seek({})'.format(type(self).__name__, offset))

        return offset


    def read(self, nbytes=-1):
        """Reads up to ``nbytes`` bytes from the uncompressed data stream.
        If ``nbytes < 0`` the stream is read until EOF.

        If the stream is already at EOF, ``b''`` is returned.

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

            buffer = <char *>buf.buffer + offset

            # read some bytes
            self._acq_fh()
            with nogil:
                ret = zran.zran_read(index, buffer, bufsz)
            self._rel_fh()

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


    def readinto(self, buf):
        """Reads up to ``len(buf)`` bytes directly into ``buf``, which is
        assumed to be a mutable ``bytes``-like object (e.g. a ``memoryview``
        or ``bytearray``.
        """

        cdef zran.zran_index_t *index  = &self.index
        cdef uint64_t           bufsz  = len(buf)
        cdef Py_buffer          pbuf
        cdef void              *vbuf
        cdef int64_t            ret

        # Create a Py_Buffer which allows
        # us to access the memory managed
        # by the provided buf
        PyObject_GetBuffer(buf, &pbuf, PyBUF_SIMPLE | PyBUF_ANY_CONTIGUOUS)

        # read some bytes
        self._acq_fh()
        try:

            vbuf = <void *>pbuf.buf
            with nogil:
                ret = zran.zran_read(index, vbuf, bufsz)

        # release the py_buffer
        finally:
            PyBuffer_Release(&pbuf)
            self._rel_fh()

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
            return 0

        # Return the number of bytes that
        # were read
        else:
            return ret


    def pread(self, nbytes, offset):
        """Seeks to the specified ``offset``, then reads and returns
        ``nbytes``. See :meth:`seek` and :meth:`read`.
        """
        IndexedGzipFile.seek(self, offset)
        return IndexedGzipFile.read(self, nbytes)


    def readline(self, size=-1):
        """Read and return up to the next ``'\n'`` character (up to at most
        ``size`` bytes, if ``size >= 0``) from the uncompressed data stream.

        If the end of the stream has been reached, ``b''`` is returned.
        """

        if size == 0:
            return bytes()

        linebuf  = b''
        startpos = self.tell()
        bufsz    = 1024

        # Read in chunks of [bufsz] bytes at a time
        while True:

            buf = self.read(bufsz)

            lineidx  = buf.find(b'\n')
            haveline = lineidx  >= 0
            eof      = len(buf) == 0

            # Are we at EOF? Nothing more to do
            if eof:
                break

            # Have we found a line? Discard
            # everything that comes after it
            if haveline:
                linebuf = linebuf + buf[:lineidx + 1]

            # If we've found a line, and are
            # not size-limiting, we're done
            if haveline and size < 0:
                break

            # If we're size limiting, and have
            # read in enough bytes, we're done
            if size >= 0 and len(linebuf) > size:
                linebuf = linebuf[:size]
                break

        # Rewind the seek location
        # to the finishing point
        self.seek(startpos + len(linebuf))

        return linebuf


    def readlines(self, hint=-1):
        """Reads and returns a list of lines from the uncompressed data.
        If ``hint`` is provided, lines will be read until the total size
        of all lines exceeds ``hint`` in bytes.
        """

        totalsize = 0
        lines     = []

        while True:

            line = self.readline()
            if line == b'':
                break

            lines.append(line)

            totalsize += len(line)

            if hint >= 0 and totalsize > hint:
                break

        return lines


    def __iter__(self):
        """Returns this ``IndexedGzipFile`` which can be iterated over to
        return lines (separated by ``'\n'``) in the uncompressed stream.
        """
        return self


    def __next__(self):
        """Returns the next line from the uncompressed stream. Raises
        :exc:`StopIteration` when there are no lines left.
        """
        line = self.readline()

        if line == b'':
            raise StopIteration()
        else:
            return line


    def write(self, *args, **kwargs):
        """Currently raises a :exc:`NotImplementedError`."""
        raise NotImplementedError('IndexedGzipFile does not support writing')


    def flush(self):
        """Currently does nothing. """
        pass


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


class SafeIndexedGzipFile(io.BufferedReader):
    """The ``SafeIndexedGzipFile`` is an ``io.BufferedReader`` which wraps
    an :class:`IndexedGzipFile` instance. By accessing the ``IndexedGzipFile``
    instance through an ``io.BufferedReader``, read performance is improved
    through buffering, and access to the I/O methods is made thread-safe.

    A :meth:`pread` method is also implemented, as it is not implemented by
    the ``io.BufferedReader``.
    """


    def __init__(self, *args, **kwargs):
        """Opens an ``IndexedGzipFile``, and then calls
        ``io.BufferedReader.__init__``.

        :arg buffer_size: Optional, must be passed as a keyword argument.
                          Passed through to ``io.BufferedReader.__init__``.
                          If not provided, a default value of 1048576 is used.

        All other arguments are passed through to ``IndezedGzipFile.__init__``.
        """

        buffer_size     = kwargs.pop('buffer_size', 1048576)
        fobj            = IndexedGzipFile(*args, **kwargs)
        self.__fileLock = threading.RLock()

        super(SafeIndexedGzipFile, self).__init__(fobj, buffer_size)


    def pread(self, nbytes, offset):
        """Seeks to ``offset``, then reads and returns up to ``nbytes``.
        The calls to seek and read are protected by a ``threading.RLock``.
        """
        with self.__fileLock:
            self.seek(offset)
            return self.read(nbytes)
