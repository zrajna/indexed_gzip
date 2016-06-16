#
# The IndexedGzipFile class.
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#

"""This module provides the IndexedGzipFile class, a drop-in replacement
for the built-in gzip.GzipFile class, for faster read-only random access
to gzip files.
"""


__version__ = '0.1'


from libc.stdio  cimport (SEEK_SET,
                          FILE,
                          fdopen)

from cpython.mem cimport (PyMem_Malloc,
                          PyMem_Realloc,
                          PyMem_Free)

cimport zran


class NotCoveredError(Exception):
    """Exception raised by the IndexedGzipFile when an attempt is made to seek
    to/read from a location that is not covered by the index. This exception
    will never be raised if the IndexedGzipFile was created with
    auto_build=True.
    """
    pass


class ZranError(Exception):
    """Exception raised by the IndexedGzipFile when the zran library signals
    an error.
    """
    pass


cdef class IndexedGzipFile:
    """The IndexedGzipFile class allows for fast random access of a gzip file
    by using the zran library to build and maintain an index of seek points
    into the file.
    """

    
    cdef zran.zran_index_t index
    """A reference to the zran_index struct. """

    
    cdef bint auto_build
    """Flag which is set to True if the file index is built automatically on
    seeks/reads.
    """

    
    cdef bint own_file
    """Flag which is set to True if the user specified a file name instead of
    an open file handle. In this case, the IndexedGzipFile is responsible for
    closing the file handle when it is destroyed.
    """

    
    cdef FILE *cfid
    """A reference to the C file handle. """

    
    cdef object pyfid
    """A reference to the python file handle. """
    

    def __cinit__(self,
                  fid=None,
                  filename=None,
                  auto_build=True,
                  spacing=1048576,
                  window_size=32768,
                  readbuf_size=16384):
        """Create an IndexedGzipFile. The file may be specified either with an
        open file handle, or with a filename. If the former, the file must have
        been opened in 'rb' mode.

        :arg fid:          Open file handle.
        
        :arg filename:     File name.
        
        :arg auto_build:   If True (the default), the index is automatically 
                           built on seeks/reads.
        
        :arg spacing:      Number of bytes between index seek points.
        
        :arg window_size:  Number of bytes of uncompressed data stored with
                           each seek point.
        
        :arg readbuf_size: Size of buffer in bytes for storing compressed data
                           read in from the file.
        """

        if fid is None and filename is None:

            raise ValueError('One of fid or filename must be specified')

        if fid is not None and fid.mode != 'rb':
            raise ValueError('The gzip file must be opened in '
                             'read-only binary ("rb") mode')

        self.own_file   = fid is None
        self.auto_build = auto_build
        
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


    def close(self):
        """Closes this IndexedGzipFile. """

        if self.closed():
            raise IOError('IndexedGzipFile is already closed')

        zran.zran_free(&self.index)

        if self.own_file:
            self.pyfid.close()
        
        self.cfid  = NULL
        self.pyfid = None

        
    def closed(self):
        """Returns True if this IndexedGzipFile is closed, False otherwise. """
        return self.pyfid is None


    def readable(self):
        """Returns True if this IndexedGzipFile is readable, False otherwise.
        """
        return not self.closed()

    
    def writable(self):
        """Always returns False - the IndexedGzipFile does not support writing.
        """
        return False

    
    def seekable(self):
        """Returns True if this IndexedGzipFile supports seeking, False
        otherwise.
        """
        return not self.closed()

    
    def tell(self):
        """Returns the current seek offset into the uncompressed data stream.
        """
        return zran.zran_tell(&self.index)

    
    def __enter__(self):
        """Returns this IndexedGzipFile. """
        return self


    def __exit__(self):
        """Calls close on this IndexedGzipFile. """
        if not self.closed():
            self.close()

    
    def __dealloc__(self):
        """Frees the memory used by this IndexedGzipFile. If a file name was
        passed to __cinit__, the file handle is closed.
        """
        if not self.closed():
            self.close()


    def build_full_index(self):
        """Re-builds the full file index. """
        
        if zran.zran_build_index(&self.index, 0, 0) != 0:
            raise ZranError('zran_build_index returned error')


    def seek(self, offset):
        """Seeks to the specified position in the uncompressed data stream.

        If this IndexedGzipFile was created with auto_build=False, and the
        requested offset is not covered by the index, a NotCoveredError is
        raised.
        """

        ret = zran.zran_seek(&self.index, offset, SEEK_SET, NULL)

        if ret < 0:
            raise ZranError('zran_seek returned error')
        
        elif ret > 0:
            raise NotCoveredError('Index does not cover '
                                  'offset {}'.format(offset))
        

    def read(self, nbytes):
        """Reads up to nbytes bytes from the uncompressed data stream. """

        buf = ReadBuffer(nbytes)
        ret = zran.zran_read(&self.index, buf.buffer, nbytes)
        
        if ret < -1:
            raise ZranError('zran_read returned error')
        
        elif ret == -1:
            raise NotCoveredError('Index does not cover current offset')

        if ret == 0:
            return bytes()

        buf.resize(ret)
        pybuf = <bytes>(<char *>buf.buffer)[:ret]

        return pybuf


    def write(self, *args, **kwargs):
        """Raises a ``NotImplementedError``."""
        raise NotImplementedError('IndexedGzipFile does not support writing')


cdef class ReadBuffer:
    """Wrapper around a chunk of memory.
 
    .. see:: http://docs.cython.org/src/tutorial/memory_allocation.html
    """

    cdef void *buffer
    """A raw chunk of bytes. """

    
    def __cinit__(self, size_t size):
        """Allocate ``size`` bytes of memory. """

        self.buffer = PyMem_Malloc(size);

        if not self.buffer:
            raise MemoryError('PyMem_Malloc fail')


    def resize(self, size_t size):
        """Re-allocate the memory to the given ``size``. """
        
        buf = PyMem_Realloc(self.buffer, size)

        if not buf:
            raise MemoryError('PyMem_Realloc fail')

        self.buffer = buf


    def __dealloc__(self):
        """Free the mwmory. """
        PyMem_Free(self.buffer)
