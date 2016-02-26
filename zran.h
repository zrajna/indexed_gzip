#ifndef __ZRAN_H__
#define __ZRAN_H__

#include <stdlib.h>


#define WINSIZE 32768U      /* sliding window size */
#define CHUNK 16384         /* file input buffer size */


struct _zran_point;
struct _zran_index;


typedef struct _zran_point zran_point_t;
typedef struct _zran_index zran_index_t;


/* access point entry */
struct _zran_point {
    
    off_t         uncmp_offset;     /* corresponding offset in uncompressed data */
    off_t         cmp_offset;       /* offset in input file of first full byte */
    int           bits;             /* number of bits (1-7) from byte at in - 1, or 0 */
    unsigned char window[WINSIZE];  /* preceding 32K of uncompressed data */
};


/* access point list */
struct _zran_index {

    int           span;  
    int           have; /* number of list entries filled in */
    int           size; /* number of list entries allocated */
    zran_point_t *list; /* allocated list */
    off_t         uncmp_seek_offset;
};

void zran_new(zran_index_t *index);


int zran_init(zran_index_t *index, int span);


int zran_expand(zran_index_t *index);


int zran_free_unused(zran_index_t *index);


void zran_dealloc(zran_index_t *index);


zran_point_t * zran_get_point_at(zran_index_t *index,
                                 off_t         offset,
                                 char          compressed);


int zran_add_point(zran_index_t  *index,
                   int            bits,
                   off_t          cmp_offset,
                   off_t          uncmp_offset,
                   unsigned       left,
                   unsigned char *window);


int zran_build_full_index(zran_index_t *index, FILE *in);


int zran_seek(zran_index_t  *index,
              FILE          *in,
              off_t          offset,
              int            whence,
              zran_point_t **point);


int zran_read(zran_index_t  *index,
              FILE          *in,
              unsigned char *buf,
              int            len);


int zran_extract(zran_index_t  *index,
                 FILE          *in,
                 off_t          offset,
                 unsigned char *buf,
                 int            len);

#endif /* __ZRAN_H__ */
