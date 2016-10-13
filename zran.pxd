#
# Cython declaration for the zran library.
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#

from libc.stdio  cimport FILE
from libc.stdint cimport uint16_t, uint32_t, uint64_t, int64_t
from posix.types cimport off_t


cdef extern from "zran.h":

    ctypedef struct zran_index_t:
        pass
    
    ctypedef struct zran_point_t:
        pass

    enum:
        ZRAN_AUTO_BUILD       =  1,
        ZRAN_SEEK_FAIL        = -1,
        ZRAN_SEEK_OK          =  0,
        ZRAN_SEEK_NOT_COVERED =  1,
        ZRAN_SEEK_EOF         =  2,

        ZRAN_READ_NOT_COVERED = -1,
        ZRAN_READ_EOF         = -2,
        ZRAN_READ_FAIL        = -3,

    bint zran_init(zran_index_t *index,
                   FILE         *fd,
                   uint32_t      spacing,
                   uint32_t      window_size,
                   uint32_t      readbuf_size,
                   uint16_t      flags)
                  
    void zran_free(zran_index_t *index)

    bint zran_build_index(zran_index_t *index,
                          uint64_t      from_,
                          uint64_t      until) nogil;

    long zran_tell(zran_index_t *index);

    int zran_seek(zran_index_t *index,
                  off_t         offset,
                  int           whence,
                  zran_point_t *point) nogil;

    int64_t zran_read(zran_index_t *index,
                      void         *buf,
                      uint64_t      len) nogil;
