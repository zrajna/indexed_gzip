#
# Cython declaration for the zran library.
#
# Author: Paul McCarthy <pauldmccarthy@gmail.com>
#

from libc.stdio  cimport FILE
from libc.stdint cimport uint16_t, uint32_t, uint64_t
from posix.types cimport off_t


cdef extern from "zran.h":

    ctypedef struct zran_index_t:
        pass
    
    ctypedef struct zran_point_t:
        pass

    enum: ZRAN_AUTO_BUILD

    bint zran_init(zran_index_t *index,
                   FILE         *fd,
                   uint32_t      spacing,
                   uint32_t      window_size,
                   uint32_t      readbuf_size,
                   uint16_t      flags)
                  
    void zran_free(zran_index_t *index)

    bint zran_build_index(zran_index_t *index,
                          uint64_t      from_,
                          uint64_t      until)

    int zran_seek(zran_index_t *index,
                  off_t         offset,
                  int           whence,
                  zran_point_t *point)

    int zran_read(zran_index_t *index,
                  void         *buf,
                  size_t        len) 
