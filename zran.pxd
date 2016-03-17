from libc.stdio import *


cdef extern from "stdio.h":
    ctypedef void* FILE

    
cdef extern from "zran.h":

    ctypedef struct zran_index_t:
        pass
    
    ctypedef struct zran_point_t:
        pass

    int ZRAN_AUTO_BUILD

    bint zran_init(zran_index_t *index,
                   FILE         *fd,
                   int           spacing,
                   int           window_size,
                   int           readbuf_size,
                   int           flags)
                  
    void zran_free(zran_index_t *index)

    bint zran_build_index(zran_index_t *index,
                          long          from_,
                          long          until)

    int zran_seek(zran_index_t *index,
                  long          offset,
                  int           whence,
                  zran_point_t *point)

    int zran_read(zran_index_t *index,
                  void         *buf,
                  int           len) 
