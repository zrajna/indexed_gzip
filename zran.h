#ifndef __ZRAN_H__
#define __ZRAN_H__

/*
 *
 */

#include <stdlib.h>
#include <stdint.h>


#define CHUNK   16384       /* file input buffer size */


struct _zran_point;
struct _zran_index;


typedef struct _zran_point zran_point_t;
typedef struct _zran_index zran_index_t;


/* access point entry */
struct _zran_point {
    
    uint64_t  uncmp_offset; /* corresponding offset in uncompressed data */
    uint64_t  cmp_offset;   /* offset in input file of first full byte */
    uint8_t   bits;         /* number of bits (1-7) from byte at in - 1, or 0 */
    uint32_t  nbytes; 
    uint8_t  *data;         /* preceding chunk of uncompressed data */
};


/* access point list */
struct _zran_index {

    uint32_t      spacing;
    uint32_t      window_size;
  
    uint32_t      npoints; /* number of list entries filled in */
    uint32_t      size;    /* number of list entries allocated */
    zran_point_t *list;    /* allocated list */
  
    uint64_t      uncmp_seek_offset;
};


// Pass in spacing=0, window_size=0 to use default values.
int  zran_init(zran_index_t *index,
               uint32_t      spacing,
               uint32_t      window_size);

void zran_free(zran_index_t *index);


int zran_build_index(zran_index_t *index,
                     FILE         *in);


// Should I use stdint types for the below
// functions? Or should I just use the types
// used by fseek and read?

int zran_seek(zran_index_t  *index,
              FILE          *in,
              off_t          offset,
              int            whence,
              zran_point_t **point);


size_t zran_read(zran_index_t  *index,
                 FILE          *in,
                 uint8_t       *buf,
                 size_t         len);


#endif /* __ZRAN_H__ */
