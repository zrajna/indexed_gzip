#ifndef __ZRAN_H__
#define __ZRAN_H__
/*
 *
 */

#include <stdlib.h>

// TODO #include <stdint.h>


#define CHUNK   16384       /* file input buffer size */


struct _zran_point;
struct _zran_index;


typedef struct _zran_point zran_point_t;
typedef struct _zran_index zran_index_t;


/* access point entry */
struct _zran_point {
    
    off_t          uncmp_offset; /* corresponding offset in uncompressed data */
    off_t          cmp_offset;   /* offset in input file of first full byte */
    int            bits;         /* number of bits (1-7) from byte at in - 1, or 0 */
    int            nbytes; 
    unsigned char *data;         /* preceding chunk of uncompressed data */
};


/* access point list */
struct _zran_index {

    int           spacing;
    unsigned int  window_size;
  
    int           npoints; /* number of list entries filled in */
    int           size;    /* number of list entries allocated */
    zran_point_t *list;    /* allocated list */
  
    off_t         uncmp_seek_offset;
};


// Pass in spacing=0, window_size=0 to use default values.
int  zran_init(zran_index_t *index, int spacing, int window_size);

void zran_free(zran_index_t *index);


int zran_build_index(zran_index_t *index, FILE *in);


int zran_seek(zran_index_t  *index,
              FILE          *in,
              off_t          offset,
              int            whence,
              zran_point_t **point);


int zran_read(zran_index_t  *index,
              FILE          *in,
              unsigned char *buf,
              int            len);


#endif /* __ZRAN_H__ */
