from libc.stdio cimport *

from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free

cdef extern from "stdio.h":
    ctypedef void *FILE
    FILE *fdopen(int, const char *)
    int SEEK_SET


cimport zran


class NotCoveredError(object):
    pass
    


cdef class IndexedGzipFile:
    cdef zran.zran_index_t index

    def __cinit__(self,
                  fid,
                  spacing=1048576,
                  window_size=32768,
                  readbuf_size=16384,
                  auto_build=False):

        cdef FILE *cfid
        cdef int flags

        if auto_build:
            flags = zran.ZRAN_AUTO_BUILD

        cfid = fdopen(fid.fileno(), 'rb')
        
        if zran.zran_init(&self.index,
                          cfid,
                          spacing,
                          window_size,
                          readbuf_size,
                          flags):
            raise RuntimeError()


    def seek(self, offset):

        ret = zran.zran_seek(&self.index, offset, SEEK_SET, NULL)

        if ret < 0:
            raise RuntimeError()
        elif ret > 0:
            raise NotCoveredError()
        

    def read(self, nbytes):

        cdef void *buf = PyMem_Malloc(nbytes);

        if not buf:
            raise MemoryError()

        ret = zran.zran_read(&self.index, buf, nbytes)

        if ret <= 0:
            PyMem_Free(buf)

        if   ret <  -1: raise RuntimeError() 
        elif ret == -1: raise NotCoveredError()
        elif ret ==  0: pybuf = bytes()
        elif ret >   0:
            buf   = PyMem_Realloc(buf, ret)
            pybuf = <bytes>(<char *>buf)[:ret]

        return pybuf

    
    def __dealloc__(self):
        zran.zran_free(&self.index)
