/*
 * zran.c - indexed access to gzip files.
 * 
 * See zran.h for documentation.
 *
 * This module was originally based on the zran example, written by Mark
 * Alder, which ships with the zlib source code.
 *
 * Author: Paul McCarthy <pauldmccarthy@gmail.com>
 */
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <fcntl.h>
#include "zlib.h"

#include "zran.h"


/*
 * Turn this on to make noise.
 * 
 * #define ZRAN_VERBOSE 
 */
//#define ZRAN_VERBOSE 


#ifdef ZRAN_VERBOSE
#define zran_log(...) fprintf(stderr, __VA_ARGS__)
#else
#define zran_log(...) 
#endif


/*
 * Discards all points in the index which come after the specfiied
 * compresesd offset.
 *
 * Returns 0 on success, non-0 on failure.
 */
static int _zran_invalidate_index(
    zran_index_t *index, /* The index                       */
    uint64_t      from   /* Offset into the compressed data */
);


/*
 * Expands the capacity of the memory used to store the index ilst.
 *
 * Returns 0 on success, non-0 on failure.
 */
static int _zran_expand_point_list(
    zran_index_t *index /* The index */
);


/*
 * Reduces the capacity of the memory used to store the index list, so that it
 * is only as big as necessary.
 *
 * Returns 0 on success, non-0 on failure.
 */
static int _zran_free_unused(
    zran_index_t *index  /* The index */
);


/*
 * Searches for the zran_point which preceeds the given offset. The offset
 * may be specified as being relative to the start of the compressed data, 
 * or the uncompressed data.
 *
 * Returns:
 *
 *   - 0 on success. 
 *
 *   - A negative number if something goes wrong.
 *
 *   - A positive number to indicate that an index covering the specified
 *     offset has not yet been created.
 */
static int _zran_get_point_at(
    zran_index_t  *index,      /* The index */
    
    uint64_t       offset,     /* The desired offset into the compressed or
                                  uncompressed data stream */
    
    uint8_t        compressed, /* Pass in 0 or non-0 to indicate that the
                                  offset is relative to the uncompressed or
                                  compressed data streams, respectively. */
    
    zran_point_t **point       /* If an index point corresponding to the
                                  specified offset is identified, this pointer
                                  will be updated to point to it. */
);


/* 
 * If the index has been created without the ZRAN_AUTO_BUILD flag, this
 * function is identical to the _zran_get_point_at function.
 *
 * If the index has been created with the ZRAN_AUTO_BUILD flag, and the
 * requested offset is beyond the current range of the index, the index will
 * be expanded to encompass it.
 *
 * The input arguments and return values are identical to the
 * _zran_get_point_at function, however if the index has been built with the
 * ZRAN_AUTO_BUILD flag, this function will never return a positive number.
 */
static int _zran_get_point_with_expand(
    zran_index_t  *index,      /* The index                           */
    uint64_t       offset,     /* Desired offset                      */
    uint8_t        compressed, /* Compressed or uncompressed offset   */
    zran_point_t **point       /* Place to store the identified point */
);


/*
 * Estimate an offset in the compressed / uncompressed data stream
 * corresponding to the given offset, which is specified in the uncompressed /
 * compressed data stream.  If the given offset is specified relative to the
 * compressed data stream, the returned value is a location in the
 * uncompressed data stream which approximately corresponds to the given
 * offset.
 *
 * This function is used by the zran_seek function, if the index has been
 * created with the ZRAN_AUTO_BUILD flag, to determine how far the index needs
 * to be expanded to cover a requested offset that is not yet covered.
 */
static uint64_t _zran_estimate_offset(
    zran_index_t *index,      /* The index */
    
    uint64_t      offset,     /* The offset for which a corresponding offset
                                 is to be estimated. */
    
    uint8_t       compressed  /* Pass in 0 or non-0 to indicate that the given
                                 offset is specified relative to the
                                 uncompressed or compressed stream,
                                 respectively. */
);


/*
 * Used by _zran_expand_index, and _zran_read. Seeks to the specified
 * compressed data offset, and initialises zlib to start
 * decompressing/inflating from said offset.
 *
 * Returns 0 for success, non-0 on failure.
 */
static int _zran_init_zlib_inflate(
    zran_index_t *index,      /* The index */
    
    z_stream     *stream,     /* Pointer to a z_stream struct */

    zran_point_t *point       /* Pass in NULL to initialise for inflation from 
                                 the beginning of the stream. Or pass a 
                                 pointer to the index point corresponding to 
                                 the location to start from. */
);  


/*
 * Expands the index from its current end-point until the given offset (which
 * must be specified relative to the compressed data stream).
 *
 * Returns 0 on success, non-0 on failure.
 */
static int _zran_expand_index(
    zran_index_t *index, /* The index                      */
    uint64_t      until  /* Expand the index to this point */
);


/*
 * Adds a new point to the end of the index.
 */
static int _zran_add_point(
    zran_index_t *index,        /* The index */
    
    uint8_t       bits,         /* If the compressed and uncompressed offsets
                                   are not byte-aligned, this is the number
                                   of bits in the compressed data, before the
                                   cmp_offset, where the point is located. */
    
    uint64_t      cmp_offset,   /* Offset into the compressed data. */
    
    uint64_t      uncmp_offset, /* Offset into the uncompressed data. */
    
    uint32_t      data_offset,  /* Offset into the data pointer specifying the
                                   point at which the uncompressed data
                                   associated with this point begins - see
                                   _zran_expand_index. It is assumed that the
                                   uncompressed data wraps around this
                                   offset. */

    uint32_t      data_size,    /* Number of bytes in data */
    
    uint8_t      *data          /* Pointer to data_size bytes of uncompressed 
                                   data preceeding this index point. */
);


/* _zran_find_next_stream return codes */
int ZRAN_FIND_STREAM_ERROR     = -2;
int ZRAN_FIND_STREAM_NOT_FOUND = -1;

/*
 * This function is used to search for concatenated compressed streams.  It
 * searches through the compressed data (pointed to by stream->next_in) to
 * find the location of the next compressed stream.
 *
 * If a new stream was found, the z_stream struct is re-initialised to
 * decompress data from the new stream. In this case, this function returns
 * the number of bytes in the compressed data that were read before the stream
 * was found.
 * 
 * Otherwise (if a compressed stream was not found), this function returns 
 * ZRAN_FIND_STREAM_NOT_FOUND.
 *
 * If an error occurs, ZRAN_FIND_STREAM_ERROR is returned.
 */
static int _zran_find_next_stream(
    zran_index_t *index, /* The index           */
    z_stream     *stream /* The z_stream struct */
);


/* _zran_inflate return codes */
int ZRAN_INFLATE_ERROR          = -5;
int ZRAN_INFLATE_NOT_COVERED    = -4;
int ZRAN_INFLATE_OUTPUT_FULL    = -3;
int ZRAN_INFLATE_BLOCK_BOUNDARY = -2;
int ZRAN_INFLATE_EOF            = -1;
int ZRAN_INFLATE_OK             =  0;

/* 
 * _zran_inflate input flags. 
 * Bit position, as a power of 2 
 */
uint32_t ZRAN_INFLATE_INIT_Z_STREAM         = 1;
uint32_t ZRAN_INFLATE_FREE_Z_STREAM         = 2;
uint32_t ZRAN_INFLATE_INIT_READBUF          = 4;
uint32_t ZRAN_INFLATE_FREE_READBUF          = 8;
uint32_t ZRAN_INFLATE_USE_OFFSET            = 16;
uint32_t ZRAN_INFLATE_STOP_AT_BLOCK         = 32;
uint32_t ZRAN_INFLATE_CLEAR_READBUF_OFFSETS = 64;


/* Macros used by _zran_inflate for testing flags. */
#define inflate_init_stream(  flags) ((flags & ZRAN_INFLATE_INIT_Z_STREAM) > 0)
#define inflate_free_stream(  flags) ((flags & ZRAN_INFLATE_FREE_Z_STREAM) > 0)
#define inflate_init_readbuf( flags) ((flags & ZRAN_INFLATE_INIT_READBUF)  > 0)
#define inflate_free_readbuf( flags) ((flags & ZRAN_INFLATE_FREE_READBUF)  > 0)
#define inflate_use_offset(   flags) ((flags & ZRAN_INFLATE_USE_OFFSET)    > 0)
#define inflate_stop_at_block(flags) ((flags & ZRAN_INFLATE_STOP_AT_BLOCK) > 0)
#define inflate_clear_readbuf_offsets(flags) \
    ((flags & ZRAN_INFLATE_CLEAR_READBUF_OFFSETS) > 0)
 

/*
 * Inflate (decompress) the specified number of bytes, or until the next
 * Z_BLOCK/Z_STREAM_END is reached.
 *
 * This is a complicated function which implements the core decompression
 * routine, and is used by both _zran_expand_index, and zran_read.
 *
 * 
 *
 * Flags can be a combination of ZRAN_INFLATE_MANAGE_Z_STREAM and
 * ZRAN_INFLATE_STOP_AT_BLOCK
 *
 * Returns 0 on success - this indicates that either the output buffer has
 * been filled, or EOF has been reached.
 * 
 * Otherwise returns one of the error/status codes. If total_output is not 
 * NULL, it is updated with the total number of bytes that were decompressed 
 * (and copied into data).
 *
 * Flags:
 *   - ZRAN_INFLATE_INIT_Z_STREAM
 *   - ZRAN_INFLATE_FREE_Z_STREAM
 *   - ZRAN_INFLATE_INIT_READBUF
 *   - ZRAN_INFLATE_FREE_READBUF
 *   - ZRAN_INFLATE_USE_OFFSET
 *   - ZRAN_INFLATE_STOP_AT_BLOCK
 *   - ZRAN_INFLATE_CLEAR_READBUF_OFFSETS
 *
 * Return codes:
 *   - ZRAN_INFLATE_NOT_COVERED
 *   - ZRAN_INFLATE_OUTPUT_FULL
 *   - ZRAN_INFLATE_BLOCK_BOUNDARY
 *   - ZRAN_INFLATE_EOF
 *   - ZRAN_INFLATE_ERROR
 */
static int _zran_inflate(
    zran_index_t *index,          /* Pointer to the index. */
    
    z_stream     *strm,           /* Pointer to a z_stream struct. */
    
    uint64_t      offset,         /* Compressed data offset to start inflation 
                                     from. */
    
    uint16_t      flags,          /* Control flags. */
    
    uint32_t     *total_consumed, /* Pointer which is updated to contain the 
                                     total number of bytes that were read 
                                     from the input file. */
    
    uint32_t     *total_output,   /* Pointer which is updated to contain the 
                                     total number of bytes that were inflated,
                                     and stored in data. */
    
    uint32_t      len,            /* Maximum number of bytes to inflate. May 
                                     be 0. */
    
    uint8_t      *data            /* Place to store the inflated bytes. */
);


/* Initialise a zran_index_t struct for use with the given GZIP file. */
int zran_init(zran_index_t *index,
              FILE         *fd,
              uint32_t      spacing,
              uint32_t      window_size,
              uint32_t      readbuf_size,
              uint16_t      flags)
{

    zran_point_t *point_list = NULL;
    int64_t       compressed_size;

    zran_log("zran_init(%u, %u, %u, %u)\n",
             spacing, window_size, readbuf_size, flags);

    if (spacing      == 0) spacing      = 1048576;
    if (window_size  == 0) window_size  = 32768;
    if (readbuf_size == 0) readbuf_size = 16384;

    /* 
     * The zlib manual specifies that a window size of 32KB is 'always enough'
     * to initialise inflation/deflation with a set dictionary. Less than
     * that is not guaranteed to be enough.
    */
    if (window_size < 32768)
        goto fail;

    /* 
     * window_size bytes of uncompressed data are stored with each seek point
     * in the index. So it's a bit silly to have the distance between
     * consecutive points less than the window size.
     */
    if (spacing <= window_size)
      goto fail;

    /* The file must be opened in read-only mode */
    if ((fcntl(fileno(fd), F_GETFL) & O_ACCMODE) != O_RDONLY)
      goto fail;

    /*
     * Calculate the size of the compressed file
     */
    if (fseeko(fd, 0, SEEK_END) != 0)
        goto fail;
        
    compressed_size = ftello(fd);

    if (compressed_size < 0)
        goto fail;

    if (fseeko(fd, 0, SEEK_SET) != 0)
        goto fail;

    /*
     * Allocate some initial space 
     * for the index point list
     */
    point_list = calloc(1, sizeof(zran_point_t) * 8);
    if (point_list == NULL) {
        goto fail;
    }

    /* initialise the index struct */
    index->fd                   = fd;
    index->flags                = flags;
    index->compressed_size      = compressed_size;
    index->spacing              = spacing;
    index->window_size          = window_size;
    index->log_window_size      = (int)round(log10(window_size) / log10(2));
    index->readbuf_size         = readbuf_size;
    index->readbuf_offset       = 0;
    index->readbuf_end          = 0;
    index->readbuf              = NULL;
    index->npoints              = 0;
    index->size                 = 8;
    index->uncmp_seek_offset    = 0;
    index->inflate_cmp_offset   = 0;
    index->inflate_uncmp_offset = 0;
    index->list                 = point_list;
    
    return 0;

fail:
    if (point_list == NULL)
        free(point_list);
    return -1;
};


/* Expands the memory used to store the index points. */
int _zran_expand_point_list(zran_index_t *index) {

    uint32_t new_size = index->size * 2;

    zran_log("_zran_expand_point_list(%i -> %i)\n", index->size, new_size);
    
    zran_point_t *new_list = realloc(index->list,
                                     sizeof(zran_point_t) * new_size);

    if (new_list == NULL) {
        return -1;
    }
    
    index->list = new_list;
    index->size = new_size;
    
    return 0;
};


/* Frees any unused memory allocated for index storage. */
int _zran_free_unused(zran_index_t *index) {

    zran_log("_zran_free_unused\n");

    zran_point_t *new_list;

    new_list = realloc(index->list, sizeof(zran_point_t) * index->npoints);

    if (new_list == NULL) {
        return -1;
    }
    
    index->list = new_list;
    index->size = index->npoints;

    return 0;
};


/* Deallocate memory used by a zran_index_t struct. */
void zran_free(zran_index_t *index) {

    uint32_t      i;
    zran_point_t *pt;

    zran_log("zran_free\n");

    for (i = 0; i < index->npoints; i++) {
        pt = &(index->list[i]);

        if (pt->data != NULL) {
            free(pt->data);
        }
    }
    
    if (index->list != NULL) {
        free(index->list);
    }
    
    index->fd                = NULL;
    index->spacing           = 0;
    index->window_size       = 0;
    index->readbuf_size      = 0;
    index->npoints           = 0;
    index->size              = 0;
    index->list              = NULL;
    index->uncmp_seek_offset = 0;
};


/* Discard all points in the index after the specified compressed offset. */
int _zran_invalidate_index(zran_index_t *index, uint64_t from)
{
    uint64_t      i;
    zran_point_t *p;

    if (index->npoints == 0)
        return 0;
            
    for (i = 0; i < index->npoints; i++) {

        p = &(index->list[i]);

        if (p->cmp_offset >= from) 
            break;
    }

    /* 
     * The index doesn't cover 
     * the requested offest 
     */
    if (i == index->npoints)
        return 0;

    if (i <= 1) index->npoints = 0;
    else        index->npoints = i - 1;
    
    return _zran_free_unused(index);
}


/* (Re-)Builds the full index. */
int zran_build_index(zran_index_t *index, uint64_t from, uint64_t until)
{
    
    if (_zran_invalidate_index(index, from) != 0)
        return -1;

    if (until == 0)
      until = index->compressed_size;

    return _zran_expand_index(index, until);
}


/* Searches for and returns the index at the specified offset. */
int _zran_get_point_at(
    zran_index_t  *index,
    uint64_t       offset,
    uint8_t        compressed,
    zran_point_t **point)
{
    uint64_t      cmp_max;
    uint64_t      uncmp_max;
    zran_point_t *last;
    zran_point_t *prev;
    zran_point_t *curr;
    uint8_t       bit;
    uint32_t      i;

    *point = NULL;

    zran_log("_zran_get_point_at(%llu, c=%u)\n", offset, compressed);

    /*
     * Figure out how much of the compressed and 
     * uncompressed data the index currently covers.
     */
    if (index->npoints == 0) { 
        cmp_max   = 0;
        uncmp_max = 0;
    }
    
    /*
     * We can't be exact with these limits, because 
     * the index points are only approximately spaced
     * throughout the data. So we'll err on the side 
     * of caution, and add a bit of padding to the 
     * limits.
     *
     * A bad compressed offset is easily checked 
     * against the file size, but a bad uncompressed
     * offset (i.e. one larger than the size of the 
     * uncompressed data) cannot be checked, as we
     * have no way of knowing the size of the 
     * uncompressed data. 
     *
     * If a bad uncompressed offset is passed in (one 
     * which is greater than the uncompressed data size), 
     * this function will incorrectly report that the 
     * index does not yet cover the offset, instead of 
     * just failing. 
     *
     * There's no method of getting around this which 
     * does not use heuristics/guesses, so I'm going to 
     * leave this as-is.
     */
    else {
        last      = &(index->list[index->npoints - 1]);
        uncmp_max = last->uncmp_offset + index->spacing * 2.0;
        cmp_max   = last->cmp_offset   + index->spacing;
        
        if (cmp_max > index->compressed_size)
          cmp_max = index->compressed_size;
    }

    /* 
     * Bad input, or out of the current index range.
     */
    if ( compressed && offset >= index->compressed_size) goto fail;
    if ( compressed && offset >= cmp_max)                goto not_created;
    if (!compressed && offset >= uncmp_max)              goto not_created;

    /* 
     * We should have an index point 
     * which corresponds to this offset, 
     * so let's search for it.
     */
    prev = index->list;
    for (i = 1; i < index->npoints; i++) {
        
        curr = &(index->list[i]);

        if (compressed) {

            /* 
             * Adjust the offset for non 
             * byte-aligned seek points.
             */
            if (curr->bits > 0) bit = 1;
            else                bit = 0;
            
            if (curr->cmp_offset > offset + bit) 
                break;
                
        }
        else {
            if (curr->uncmp_offset > offset) 
                break;
        }

        prev = curr;
    }

    *point = prev;
    return 0;

fail:
    *point = NULL;
    return -1;
    
not_created:
    *point = NULL; 
    return 1;
}


/* 
 * Get the index point corresponding to the given offset, expanding 
 * the index as needed if ZRAN_AUTO_BUILD is active.
 */
int _zran_get_point_with_expand(zran_index_t  *index,
                                uint64_t       offset,
                                uint8_t        compressed,
                                zran_point_t **point)
{
    
    int      result;
    uint64_t expand;

    zran_log("_zran_get_point_with_expand(%llu, %u, autobuild=%u)\n",
             offset,
             compressed,
             index->flags & ZRAN_AUTO_BUILD);

    if ((index->flags & ZRAN_AUTO_BUILD) == 0) {
        return _zran_get_point_at(index, offset, compressed, point);
    }

    /*
     * See if there is an index point that 
     * covers the specified offset. If there's
     * not, we're going to expand the index
     * until there is.
     */
    result = _zran_get_point_at(index, offset, compressed, point);
    while (result > 0) {
    
        /*
         * If result > 0, get_point says that an index 
         * point for this offset doesn't yet exist. So 
         * let's expand the index.
         *
         * Guess how far we need to expand the index,
         * and expand it by that much.
         */
        if (compressed == 0)
            expand = _zran_estimate_offset(index, offset, 0);
        else
            expand = offset;

        /* 
         * There's no point trying to 
         * expand the index beyond the 
         * size of the compressed data
         *
         */
        if (offset >= index->compressed_size)
            expand = index->compressed_size; 

        /*
         * Expand the index
         */
        if (_zran_expand_index(index, expand) != 0) {
            goto fail;
        }

        /*
         * Index has been expanded, so there 
         * should now be a point which covers 
         * the requested offset.
         */
        result = _zran_get_point_at(index, offset, compressed, point); 
 
        /*
         * If, above, we set expand to the compressed data 
         * size, it means that the index has been expanded 
         * to cover the entire file. But if _zran_get_point_at 
         * is still returning "point not created", it means 
         * that the offset that was passed in to this function 
         * is past the end of the file.
         */
        if (result > 0 && expand == index->compressed_size) {

            result = 0;
            *point = &index->list[index->npoints - 1];
        } 
    }

    /* 
     * get point failed - something has gone wrong 
     */
    if (result < 0) {
        goto fail;
    }

    /* 
     * get point returned success, 
     * but didn't give me a point. 
     * This should never happen.
     */ 
    if (result == 0 && *point == NULL) {
        goto fail;
    }

    return 0;

fail:
    return -1;
}


/* 
 * Given an offset in one stream, estimates the corresponding offset into the 
 * other stream.
 */
uint64_t _zran_estimate_offset(
    zran_index_t *index,
    uint64_t      offset,
    uint8_t       compressed)
{

    zran_point_t *last;
    uint64_t      estimate;

    /* 
     * The first index in the list maps 
     * indices 0 and 0, which won't help 
     * us here. So we need at least two 
     * index points.
     */
    if (index->npoints <= 1) last = NULL;
    else                     last = &(index->list[index->npoints - 1]);

    /*
     * We have no reference. At least two
     * index points need to have been created.
     * The assumed correspondences between
     * the compressed streams are arbitrary.
     */
    if (last == NULL) {
        if (compressed) estimate = offset * 2.0;
        else            estimate = offset * 0.8;
    }

    /*
     * I'm just assuming a roughly linear correspondence
     * between the compressed/uncompressed data streams. 
     */
    else if (compressed) {
        estimate = round(offset * ((float)last->uncmp_offset / last->cmp_offset));
    }
    else {
        estimate = round(offset * ((float)last->cmp_offset / last->uncmp_offset));
    }

    zran_log("_zran_estimate_offset(%llu, %u) = %llu\n",
             offset, compressed, estimate); 

    return estimate;
}
                          


/* Add a new point to the index. */
int _zran_add_point(zran_index_t  *index,
                    uint8_t        bits,
                    uint64_t       cmp_offset,
                    uint64_t       uncmp_offset,
                    uint32_t       data_offset,
                    uint32_t       data_size,
                    uint8_t       *data) {

    zran_log("_zran_add_point(%i, c=%lld + %i, u=%lld, data=%u / %u)\n",
             index->npoints,
             cmp_offset,
             bits > 0,
             uncmp_offset,
             data_offset,
             data_size);
    
    #ifdef ZRAN_VERBOSE
    int doff;
    if (data_offset >= index->window_size) doff = data_offset - index->window_size;
    else                                   doff = data_size = (index->window_size - data_offset);
    
    if (data != NULL)
        zran_log("Window data: [%02x %02x %02x %02x ...]\n",
                 data[(data_offset - index->window_size + 0) % data_size],
                 data[(data_offset - index->window_size + 1) % data_size],
                 data[(data_offset - index->window_size + 2) % data_size],
                 data[(data_offset - index->window_size + 3) % data_size]);
    #endif

    uint8_t      *point_data = NULL;
    zran_point_t *next       = NULL;

    /* if list is full, make it bigger */
    if (index->npoints == index->size) {
        if (_zran_expand_point_list(index) != 0) {
            goto fail;
        }
    }

    /* 
     * Allocate memory to store the 
     * uncompressed data (the "window")
     * associated with this point. The 
     * first index point (where uncmp_offset == 0)
     * has no data associated with it for 
     * obvious reasons.
     */
    if (uncmp_offset == 0) {
        point_data = NULL;
    }
    else {
        point_data = calloc(1, index->window_size);
        if (point_data == NULL)
            goto fail;
    }

    next               = &(index->list[index->npoints]);
    next->bits         = bits;
    next->cmp_offset   = cmp_offset;
    next->uncmp_offset = uncmp_offset;
    next->data         = point_data;

    /* 
     * The uncompressed data may not start at 
     * the beginning of the data pointer, but 
     * rather from an arbitrary point. So we 
     * copy the beginning of the window from 
     * the end of data, and the end of the 
     * window from the beginning of data. Does 
     * that make sense?
     */
    if (uncmp_offset > 0) {
        if (data_offset >= index->window_size) {
        
            memcpy(point_data, data + (data_offset - index->window_size), index->window_size);

            zran_log("Copy %u bytes from %u to %u\n",
                     index->window_size,
                     data_offset - index->window_size,
                     data_offset);
        }
        else {
            memcpy(point_data,
                   data + (data_size - (index->window_size - data_offset)),
                   (index->window_size - data_offset));
        
            memcpy(point_data + (index->window_size - data_offset),
                   data,
                   data_offset);

            zran_log("Copy %u bytes from %u to %u, %u bytes from %u to %u\n",
                     (index->window_size - data_offset),
                     (data_size - (index->window_size - data_offset)),
                     data_size,
                     data_offset,
                     0,
                     data_offset);
        }
    }


    index->npoints++;

    return 0;

fail:
    if (point_data != NULL) 
        free(point_data);
    
    return -1;
};


/* Initialise the given z_stream struct for decompression/inflation. */
int _zran_init_zlib_inflate(zran_index_t *index,
                            z_stream     *stream,
                            zran_point_t *point)
{

    int   ret;
    int   windowBits;
    off_t seek_loc;

    windowBits        = index->log_window_size; 
    stream->zalloc    = Z_NULL;
    stream->zfree     = Z_NULL;
    stream->opaque    = Z_NULL;
    stream->avail_in  = 0;
    stream->avail_out = 0;
    stream->next_in   = Z_NULL;

    /*
     * Seek to the required location in the compressed 
     * data stream. If the provided index point is NULL, 
     * we start from the beginning of the file.
     *
     * The compressed offset for index points correspond 
     * to the first full byte of compressed data. So if 
     * the index point is not byte-aligned (bits > 0), we 
     * need to seek to the previous byte, and tell zlib
     * about it (via the inflatePrime call below).
     */
    if (point == NULL) seek_loc = 0;
    else               seek_loc = point->cmp_offset - (point->bits > 0);

    if (fseek(index->fd, seek_loc, SEEK_SET) != 0)
        goto fail;

    /* 
     * If we're starting from the beginning 
     * of the file, we tell inflateInit2 to 
     * expect a file header
     */
    if (point == NULL) {

        zran_log("zlib_init_zlib_inflate(0, n/a, n/a, %u + 32)\n", windowBits);
        if (inflateInit2(stream, windowBits + 32) != Z_OK) {
            goto fail;
        }
    }
    
    /* 
     * Otherwise, we configure for raw inflation, 
     * and initialise the inflation dictionary
     * from the uncompressed data associated with
     * the index point.
     */
    else {

        zran_log("_zran_init_zlib_inflate(%lld, %llu, %llu, -%u)\n",
                 seek_loc,
                 point->cmp_offset,
                 point->uncmp_offset,
                 windowBits);
        
        if (inflateInit2(stream, -windowBits) != Z_OK) {
            goto fail;
        }

        /* The starting index point is not 
         * byte-aligned, so we'll insert 
         * the initial bits into the inflate 
         * stream using inflatePrime
         */
        if (point->bits > 0) {

            ret = getc(index->fd);
            
            if (ret == -1) 
                goto fail;

            if (inflatePrime(stream,
                             point->bits, ret >> (8 - point->bits)) != Z_OK)
                goto fail;
        }

        /*
         * Initialise the inflate stream 
         * with the index point data.
         */
        if (point->data != NULL) {
            if (inflateSetDictionary(stream,
                                     point->data,
                                     index->window_size) != Z_OK)
                goto fail;
        }
    }

    return 0;

fail:
    return -1;
}


/* 
 * Identify the location of the next compresesd stream (if the file 
 * contains concatenated streams).
 */
int _zran_find_next_stream(zran_index_t *index, z_stream *stream) {

    /* 
     * Search for the beginning of 
     * the next stream. GZIP files 
     * start with 0x1f8b. 
     */
    int offset = 0;
    int found  = 0;
    
    while (stream->avail_in > 2) {
        
        if (stream->next_in[0] == 0x1f &&
            stream->next_in[1] == 0x8b) {
            found = 1;
            break;
        }

        offset           += 2;
        stream->next_in  += 2;
        stream->avail_in -= 2;
    }

    /* 
     * No header found for 
     * the next stream.
     */
    if (found == 0)
        goto not_found;

    zran_log("New stream found, re-initialising inflation\n");

    /*
     * Re-configure for inflation 
     * from the new stream.
     */
    if (inflateEnd(stream) != Z_OK)
        goto fail;

    stream->zalloc    = Z_NULL;
    stream->zfree     = Z_NULL;
    stream->opaque    = Z_NULL;
    stream->avail_out = 0;

    if (inflateInit2(stream, index->log_window_size + 32) != Z_OK)
        goto fail;

    return offset;
    
fail:
    return ZRAN_FIND_STREAM_ERROR;
    
not_found:
    return ZRAN_FIND_STREAM_NOT_FOUND;
}


/* The workhorse. Inflate/decompress data from the file. */
static int _zran_inflate(zran_index_t *index,
                         z_stream     *strm,
                         uint64_t      offset,
                         uint16_t      flags,
                         uint32_t     *total_consumed,
                         uint32_t     *total_output,
                         uint32_t      len,
                         uint8_t      *data) {

    /*
     * Used to store and check return 
     * values. f_ret is for fread, 
     * z_ret is for zlib/zran functions.
     * return_val is the return value for 
     * this function.
     */
    size_t f_ret;
    int    z_ret;
    int    return_val = ZRAN_INFLATE_OK;

    /* 
     * Offsets into the compressed 
     * and uncompressed data streams,
     * and total number of bytes 
     * decompressed and output.
     */
    uint64_t cmp_offset;
    uint64_t uncmp_offset;
    uint32_t _total_consumed = 0;
    uint32_t _total_output   = 0;

    /* 
     * Index point to start from 
     * (if ZRAN_INFLATE_USE_OFFSET 
     * is active).
     */
    zran_point_t *start = NULL;

    /* 
     * If ZRAN_INFLATE_INIT_READBUF is not set, 
     * make sure that a read buffer exists.
     * 
     * If the opposite is true, the read buffer
     * from a prior call has not been cleaned up. 
     */
    if ((!inflate_init_readbuf(flags) && index->readbuf == NULL) ||
        ( inflate_init_readbuf(flags) && index->readbuf != NULL)) {
        goto fail;
    }

    zran_log("_zran_inflate(%llu, block=%u, use_offset=%u, init_stream=%u,\n"
             "              free_stream=%u, init_readbuf=%u, free_readbuf=%u,\n"
             "              clear_offsets=%u, nbytes=%u)\n",
             offset,
             inflate_stop_at_block(        flags),
             inflate_use_offset(           flags),
             inflate_init_stream(          flags),
             inflate_free_stream(          flags),
             inflate_init_readbuf(         flags),
             inflate_free_readbuf(         flags),
             inflate_clear_readbuf_offsets(flags),
             len);

    /*
     * It begins...
     *
     *   1. Figure out the starting offsets into 
     *      the compressed/uncompressed streams
     * 
     *   2. Initialise the z_stream struct (if 
     *      ZRAN_INFLATE_INIT_Z_STREAM is active)
     *
     *   3. Create a read buffer (if 
     *      ZRAN_INFLATE_INIT_READBUF is active)
     *
     *   4. 
     */

    /*
     * The compressed/uncompressed offsets are initialised in
     * one of three ways. If ZRAN_INFLATE_USE_OFFSET is active,
     * they are either:
     *
     *    - Both set to 0
     *  
     *    - Initialised according to an existing index 
     *      point that preceeds the requested offset.
     *
     * Otherwise, they are initialised from index->inflate_cmp_offset 
     * and index->inflate_uncmp_offset, which are assumed to have been
     * set in a prior call to _zran_inflate.
     */
    if (inflate_use_offset(flags)) {

        cmp_offset   = 0;
        uncmp_offset = 0;
        
        /*
         * If a non-zero offset has been specified, 
         * search the index to see if we can start 
         * inflating from a known location.
         */
        if (offset > 0) {

            /*
             * In order to successfully decompress 
             * data rom the current uncompressed seek 
             * location, we need to start decompressing 
             * from the index point which preceeds it.
             */
            z_ret = _zran_get_point_at(index, offset, 1, &start);

            if (z_ret < 0) return ZRAN_INFLATE_ERROR;
            if (z_ret > 0) return ZRAN_INFLATE_NOT_COVERED;
        }

        /*
         * Start inflating from the index point 
         * corresponding to the offset (or keep 
         * the offsets at 0 if no point was found).
         */
        if (start != NULL) {

            cmp_offset   = start->cmp_offset;
            uncmp_offset = start->uncmp_offset;
        }
    }
    
    /*
     * If ZRAN_INFLATE_USE_OFFSET is not active, 
     * we initialise from offsets which were 
     * stored on the last call to _zran_inflate.
     */
    else {
        cmp_offset   = index->inflate_cmp_offset;
        uncmp_offset = index->inflate_uncmp_offset;
    }

    zran_log("initialising to inflate from c=%llu, u=%llu\n",
             cmp_offset,
             uncmp_offset);

    /*
     * If ZRAN_INFLATE_INIT_Z_STREAM is active, 
     * initialise the zlib struct for inflation.
     * The _zran_init_zlib_inflate function 
     * seeks to the correct location in the file 
     * for us.
     * 
     * If ZRAN_INFLATE_INIT_Z_STREAM is not 
     * active, we assume that the file is
     * already at the correct spot.
     */
    if (inflate_init_stream(flags)) {
        if (_zran_init_zlib_inflate(index, strm, start) != 0) {
            goto fail;
        }
    }

    /*
     * If ZRAN_INFLATE_INIT_READBUF, 
     * allocate memory for reading 
     * compressed data from the file.
     * The buffer is attached to the 
     * zran_index_t->readbuf pointer.
     */
    if (inflate_init_readbuf(flags)) {
        index->readbuf = calloc(1, index->readbuf_size);
        if (index->readbuf == NULL)
            goto fail;
    }

    /*
     * If ZRAN_INFLATE_CLEAR_READBUF_OFFSETS,
     * we clear any stored information about 
     * the read buffer, and start reading 
     * from/writing to it from the beginning.
     */
    if (inflate_clear_readbuf_offsets(flags)) {
        index->readbuf_offset = 0;
        index->readbuf_end    = 0;
    }

    /*
     * Otherwise, assume that there is already 
     * some input (compressed) data in the 
     * readbuf, and that index->readbuf_offset
     * and index->readbuf_end were sert on a 
     * prior call.
     * 
     *    - readbuf_offset tells us where in
     *      readbuf the data starts
     * 
     *    - readbuf_end tells us where it ends.
     */
    else {
        strm->next_in  = index->readbuf     + index->readbuf_offset;
        strm->avail_in = index->readbuf_end - index->readbuf_offset;
    }

    /*
     * Tell zlib where to store 
     * the uncompressed data.
     */
    strm->avail_out = len;
    strm->next_out  = data;
    
    /*
     * Keep going until we run out of space.
     */
    while (strm->avail_out > 0) {

        /* We need to read in more data */
        if (strm->avail_in == 0) {

            if (feof(index->fd)) {
                return_val = ZRAN_INFLATE_EOF;
                break;
            }

            zran_log("Reading from file %llu [ == %llu?]\n",
                     ftello(index->fd), cmp_offset);
            
            /* Read a block of compressed data */
            f_ret = fread(index->readbuf, 1, index->readbuf_size, index->fd);

            if (ferror(index->fd)) goto fail;
            if (f_ret == 0)        goto fail;

            index->readbuf_end = f_ret;

            zran_log("Read %lu bytes from file [c=%llu, u=%llu]\n",
                     f_ret, cmp_offset, uncmp_offset);

            /*
             * Tell zlib about the block 
             * of compressed data that we
             * just read in.
             */
            strm->avail_in = f_ret;
            strm->next_in  = index->readbuf;
        }

        /*
         * Decompress the block until it is
         * gone (or we've read enough bytes)
         */
        z_ret = Z_OK;
        while (strm->avail_in > 0) {

            /*
             * Re-initialise inflation if we have
             * hit a new compressed stream.
             */
            if (z_ret == Z_STREAM_END) {

                zran_log("End of stream - searching for another stream\n");

                z_ret = _zran_find_next_stream(index, strm);

                /* 
                 * If _zran_find_next_stream can't find 
                 * a new stream, we are either out of 
                 * compressed input data, or at eof. In 
                 * either case, break and let the outer 
                 * loop deal with it.
                 */
                if      (z_ret == ZRAN_FIND_STREAM_NOT_FOUND) break; 
                else if (z_ret == ZRAN_FIND_STREAM_ERROR)     goto fail;

                /* 
                 * _zran_find_next_stream has found a 
                 * new stream, and has told us how many 
                 * bytes it skipped over before finding 
                 * it.
                 */
                cmp_offset      += z_ret;
                _total_consumed += z_ret;
            }

            /* 
             * Optimistically update offsets - 
             * we will adjust them after the 
             * inflate call.
             */
            cmp_offset      += strm->avail_in;
            uncmp_offset    += strm->avail_out;
            _total_consumed += strm->avail_in;
            _total_output   += strm->avail_out;

            zran_log("Before inflate - avail_in=%u, avail_out=%u\n",
                     strm->avail_in, strm->avail_out);

            /*
             * Inflate the block - the decompressed
             * data is output straight to the provided
             * data buffer.
             * 
             * If ZRAN_INFLATE_STOP_AT_BLOCK is active,
             * Z_BLOCK tells inflate to stop inflating
             * at a compression block boundary. Otherwise,
             * inflate stops when it comes to the end of a 
             * stream, or it runs out of input or output.
             */
            if (inflate_stop_at_block(flags)) z_ret = inflate(strm, Z_BLOCK);
            else                              z_ret = inflate(strm, Z_NO_FLUSH);

            zran_log("After inflate - avail_in=%u, avail_out=%u\n",
                     strm->avail_in, strm->avail_out);

            /*
             * Adjust our offsets according to what
             * was actually consumed/decompressed.
             */
            cmp_offset      -= strm->avail_in;
            uncmp_offset    -= strm->avail_out;
            _total_consumed -= strm->avail_in;
            _total_output   -= strm->avail_out;

            /*
             * Now we need to figure out what just happened.
             *
             * Z_BUF_ERROR indicates that the output buffer 
             * is full; we clobber it though, as it makes the 
             * code below a bit easier (and anyway, we can 
             * tell if the output buffer is full by checking 
             * strm->avail_out).
             */
            if (z_ret == Z_BUF_ERROR) {
                z_ret = Z_OK;
            }

            /* 
             * If z_ret is not Z_STREAM_END or 
             * Z_OK, something has gone wrong.
             */
            if (z_ret != Z_OK && z_ret != Z_STREAM_END) {
                zran_log("zlib inflate failed (code: %i, msg: %s)\n",
                         z_ret, strm->msg);
                goto fail;
            }
            
            /*
             * End of block or stream?
             * 
             * If the file comprises a sequence of 
             * concatenated gzip streams, we will 
             * encounter Z_STREAM_END before the end 
             * of the file (where one stream ends and 
             * the other begins). 
             *
             * If we used Z_BLOCK above, and inflate 
             * encountered a block boundary, it indicates
             * this in the the strm->data_type field.
             *
             * If either of these things occur, and 
             * ZRAN_INFLATE_STOP_AT_BLOCK is active,
             * we want to stop.
             *
             * If at a new stream, we re-initialise
             * inflation on the next loop iteration.
             */
            if (z_ret == Z_STREAM_END ||
                ((strm->data_type & 128) && !(strm->data_type & 64))) {

                if (inflate_stop_at_block(flags)) {
                    
                    zran_log("At block or stream boundary, "
                             "stopping inflation\n");
                    
                    return_val = ZRAN_INFLATE_BLOCK_BOUNDARY;
                    break;
                }
            }

            /*
             * We've run out of space to 
             * store decompresesd data
             */
            if (strm->avail_out == 0) {

                zran_log("Output buffer full - stopping inflation\n");

                /*
                 * We return OUTPUT_FULL if we haven't 
                 * decompressed the requested number of 
                 * bytes, or ZRAN_INFLATE_STOP_AT_BLOCK 
                 * is active and we haven't yet found a 
                 * block.
                 */
                if (inflate_stop_at_block(flags) || _total_output < len) {
                    return_val = ZRAN_INFLATE_OUTPUT_FULL;
                }
                
                break;
            }

            /* 
             * End of file. The GZIP file 
             * footer takes up 8 bytes, which
             * do not get processed by the 
             * inflate function.
             */
            if (feof(index->fd) && strm->avail_in <= 8) {
                
                zran_log("End of file, stopping inflation\n");
                
                return_val = ZRAN_INFLATE_EOF;
                break;
            }

            /* 
             * Some of the code above has decided that 
             * it wants this _zran_inflate call to return. 
             */
            if (return_val != ZRAN_INFLATE_OK) {
                break;
            }
        }

        if (return_val != ZRAN_INFLATE_OK) {
            break;
        }
    }

    /*
     * If ZRAN_INFLATE_FREE_READBUF is 
     * active, clear input buffer memory
     * and offsets.
     */
    if (inflate_free_readbuf(flags)) {
        free(index->readbuf);
        index->readbuf        = NULL;
        index->readbuf_offset = 0;
        index->readbuf_end    = 0;
    }
    
    /*
     * Otherwise save the readbuf 
     * offset for next time.
     */
    else {
        index->readbuf_offset = index->readbuf_end - strm->avail_in;
    }

    /*
     * If ZRAN_INFLATE_FREE_Z_STREAM 
     * is active, do just that.
     */
    if (inflate_free_stream(flags)) {
        if (inflateEnd(strm) != Z_OK)
            goto fail;
    }

    /* 
     * Update the total number of 
     * bytes that were consumed/read
     */
    *total_consumed = _total_consumed;
    *total_output   = _total_output;

    /*
     * Update the compressed/uncompressed 
     * offsets in case we need to use them 
     * later.
     */
    index->inflate_cmp_offset   = cmp_offset;
    index->inflate_uncmp_offset = uncmp_offset;
    
    zran_log("Inflate finished - consumed=%u, output=%u,\n"
             "                   cmp_offset=%llu, uncmp_offset=%llu \n\n",
             *total_consumed, *total_output,
             cmp_offset, uncmp_offset);

    /* Phew. */
    return return_val;

fail:
    if (index->readbuf != NULL) {
        free(index->readbuf);
        index->readbuf        = NULL;
        index->readbuf_offset = 0;
        index->readbuf_end    = 0;
    }
    
    return ZRAN_INFLATE_ERROR;
}


/*
 * Expands the index to encompass the 
 * compressed offset specified by 'until'.
 */
int _zran_expand_index(zran_index_t *index, uint64_t until)
{

    /* 
     * Used to store and check return values
     * from zlib and zran functions.
     */
    int z_ret;
    
    /* Zlib stream struct */
    z_stream strm;
    
    /*
     * Number of bytes read/decompressed 
     * on each call to _zran_inflate.
     */
    uint32_t bytes_consumed;
    uint32_t bytes_output;

    /* 
     * Buffer to store uncompressed data,
     * size of said buffer, and current offset 
     * into said buffef. We wrap the buffer 
     * around to the beginning when it is 
     * filled.
     *
     * Ideally, we only want to decompress 
     * index->spacing bytes before creating a 
     * new index point. But we may have to 
     * decompress more than this before a 
     * suitable location (a block/stream 
     * boundary) is found, so we allocate 
     * more space. 
     */
    uint8_t *data        = NULL;
    uint32_t data_size   = index->spacing * 4;
    uint32_t data_offset = 0;

    /*
     * _zran_inflate control flags. We need 
     * to use different flags on the first 
     * call - first_inflate is used to track 
     * this.
     */
    uint16_t inflate_flags;
    uint8_t  first_inflate = 1; 

    /* 
     * Counters to keep track of where we are 
     * in both the compressed and uncompressed 
     * streams. last_uncmp_offset is the 
     * uncompressed data offset of the most 
     * recent point that was added to the index
     * in this call to _zran_expand_index.
     */
    uint64_t cmp_offset;
    uint64_t uncmp_offset;
    uint64_t last_uncmp_offset;
 
    /* 
     * A reference to the last point in 
     * the index. This is where we need 
     * to start decompressing data from 
     * before we can add more index points.
     */
    zran_point_t *start = NULL;

    /*
     * Tally of the number of points 
     * we have expanded the index by
     * on this call.
     */
    uint64_t points_created = 0;

    /*
     * In order to create a new index 
     * point, we need to start reading 
     * at the last index point, so that 
     * we read enough data to initialise 
     * the inflation. If we don't have 
     * at least two points, we start 
     * at the beginning of the file.
     */
    start = NULL;
    if (index->npoints > 1) {
        
        start = &(index->list[index->npoints - 1]);

        /*
         * The index already covers the requested 
         * offset. Nothing needs to be done.
         */ 
        if (until <= start->cmp_offset)
            return 0;
    }

    /* 
     * Allocate memory for the 
     * uncompressed data buffer. 
     */
    data = calloc(1, data_size);
    if (data == NULL)
        goto fail;

    /* Let's do this. */
    zran_log("_zran_expand_index(%llu)\n", until);

    /*
     * If the caller passed until == 0, 
     * we force some data to be read.
     */
    if (until == 0) {
      until = index->spacing;
    }

    /*
     * We start from the last point in 
     * the index, or the beginning of 
     * the file, if there are not enough
     * points in the index.
     */
    if (start != NULL) {

        cmp_offset        = start->cmp_offset;
        uncmp_offset      = start->uncmp_offset;
        last_uncmp_offset = uncmp_offset;
    }
    else {
        cmp_offset        = 0;
        uncmp_offset      = 0;
        last_uncmp_offset = 0;
    }

    /*
     * Don't finish until we're at the end of the 
     * file, or we've expanded the index far enough 
     * (and have created at least one new index 
     * point).
     * 
     * We require at least one point to be created,
     * because we want index points to be located at 
     * compression block boundaries, but in some data 
     * there may be a long distance between block 
     * boundaries (longer than the desired index 
     * point spacing).
     */
    while ((cmp_offset < index->compressed_size) &&
           (cmp_offset < until || points_created == 0)) {

        /* 
         * On the first call to _zran_inflate, we
         * tell it to initialise the zlib stream
         * struct, create a read buffer, and start
         * inflation from our starting point. 
         */
        if (first_inflate) {
            first_inflate = 0;
            inflate_flags = (ZRAN_INFLATE_INIT_Z_STREAM         |
                             ZRAN_INFLATE_INIT_READBUF          |
                             ZRAN_INFLATE_USE_OFFSET            |
                             ZRAN_INFLATE_CLEAR_READBUF_OFFSETS |
                             ZRAN_INFLATE_STOP_AT_BLOCK);
        }

        /*
         * On subsequent calls, we tell _zran_inflate
         * to just continue where it left off on the
         * previous call. 
         */
        else {
            inflate_flags = ZRAN_INFLATE_STOP_AT_BLOCK;
        }

        zran_log("Searching for next block boundary\n"
                 "       c=%llu, u=%llu,\n"
                 "       data_offset=%u, data_space=%u\n",
                 cmp_offset, uncmp_offset, data_offset,
                 data_size - data_offset);

        /* 
         * We wrap the data buffer around to its 
         * beginning by using some trickery with 
         * the data_offset. By doing this, the 
         * _zran_add_point function will be able 
         * to retrieve the data associated with 
         * an index point even if some of it it 
         * is contained at the end of the data 
         * buffer, and the rest at the beginning.
         */
        z_ret = _zran_inflate(index,
                              &strm,
                              cmp_offset,
                              inflate_flags,
                              &bytes_consumed,
                              &bytes_output,
                              data_size - data_offset,
                              data      + data_offset);

        cmp_offset   += bytes_consumed;
        uncmp_offset += bytes_output;
        data_offset   = (data_offset + bytes_output) % data_size;
 
        /*
         * Has the output buffer been filled, 
         * or eof been reached? If the output 
         * buffer is full, we just continue - 
         * the data_offset trickery means that
         * we can ask the _zran_inflate 
         * function to just keep filling the
         * buffer until we find a block.
         */
        if      (z_ret == ZRAN_INFLATE_OUTPUT_FULL) continue;
        else if (z_ret == ZRAN_INFLATE_EOF)         break;

        /* 
         * If z_ret != ZRAN_INFLATE_BLOCK_BOUNDARY, 
         * something has gone wrong.
         */
        else if (z_ret != ZRAN_INFLATE_BLOCK_BOUNDARY) {
            goto fail;
        }

        /* 
         * If we're at the begininng of the file
         * (uncmp_offset == 0), or at the end 
         * of a compress block and index->spacing 
         * bytes have passed since the last index 
         * point that was created, we'll create a 
         * new index point at this location.
         */
        if (uncmp_offset == 0 ||
            uncmp_offset - last_uncmp_offset >= index->spacing) {

            if (_zran_add_point(index,
                                strm.data_type & 7,
                                cmp_offset,
                                uncmp_offset,
                                data_offset,
                                data_size,
                                data) != 0) {
                goto fail;
            }

            points_created   += 1;
            last_uncmp_offset = uncmp_offset;
        }
    }

    /* 
     * A final call to _zran_inflate, to clean 
     * up read buffer and z_stream memory.
     */
    z_ret = _zran_inflate(index,
                          &strm,
                          0,
                          (ZRAN_INFLATE_CLEAR_READBUF_OFFSETS |
                           ZRAN_INFLATE_FREE_Z_STREAM         |
                           ZRAN_INFLATE_FREE_READBUF),
                          &bytes_consumed,
                          &bytes_output,
                          0,
                          data);

    if (z_ret != ZRAN_INFLATE_OK && z_ret != ZRAN_INFLATE_EOF) {
        goto fail;
    }

    /*
     * The index may have over-allocated 
     * space for storing index points, so 
     * here we free the unused memory.
     */
    if (_zran_free_unused(index) != 0) {
        goto fail;
    }

    free(data);
    return 0;

fail:
    if (data != NULL)
        free(data);
    
    return -1;
};


/*
 * Seek to the approximate location of the specified offset into 
 * the uncompressed data stream. The whence argument must be 
 * SEEK_SET or SEEK_CUR.
 */ 
int zran_seek(zran_index_t  *index,
              off_t          offset,
              int            whence,
              zran_point_t **point)
{

    int           result;
    zran_point_t *seek_point;

    zran_log("zran_seek(%lld, %i)\n", offset, whence);

    if (whence != SEEK_SET && whence != SEEK_CUR) {
        goto fail;
    }

    /* 
     * SEEK_CUR: seek relative to 
     * the current file position.
     */
    if (whence == SEEK_CUR) {
      offset += index->uncmp_seek_offset;
    }

    /* Bad input */
    if (offset < 0) {
      goto fail;
    }

    /*
     * Get the index point that 
     * corresponds to this offset.
     */
    result = _zran_get_point_with_expand(index, offset, 0, &seek_point);

    if      (result < 0) goto fail;
    else if (result > 0) goto not_covered_by_index;
    
    index->uncmp_seek_offset = offset;
    offset                   = seek_point->cmp_offset;

    /* 
     * This index point is not byte-aligned. 
     * Adjust the offset accordingly.
     */
    if (seek_point->bits > 0)
        offset -= 1;

    /* 
     * The caller wants a ref to the 
     * index point corresponding to 
     * the seek location.
     */
    if (point != NULL) {
        *point = seek_point;
    }

    return fseeko(index->fd, offset, SEEK_SET);
    
fail:
    return -1;
not_covered_by_index:
    return 1;
}

/* Return the current seek position in the uncompressed data stream. */
long zran_tell(zran_index_t *index) {
  
    return index->uncmp_seek_offset;
}


/* Read len bytes from the uncompressed data stream, storing them in buf. */
int64_t zran_read(zran_index_t *index,
                  void         *buf,
                  uint64_t      len) {

    /* Used to store/check return values. */
    int ret;

    /*
     * Number of bytes read/output on 
     * each call to _zran_inflate.
     */
    uint32_t bytes_consumed;
    uint32_t bytes_output;

    /*
     * _zran_inflate control flags. We need
     * to pass different flags on thefirst 
     * call to _zran_inflate.
     */
    uint16_t inflate_flags;
    uint8_t  first_inflate = 1;

    /* 
     * Counters keeping track of the current 
     * location in both the compressed and 
     * uncompressed streams, and the total 
     * number of bytes read.
     */
    off_t    uncmp_offset;
    off_t    cmp_offset;
    uint64_t total_read;

    /* 
     * Zlib stream struct and starting 
     * index point for the read..
     */
    z_stream      strm;
    zran_point_t *start = NULL;

    /*
     * Memory used to store bytes that we skip 
     * over before reaching the appropriate 
     * point in the uncompressed data stream.
     *
     * to_discard is used to store the number of 
     * bytes that we want to discard on a single
     * call to _zran_inflate (which is limited by
     * the discard buffer size).
     *
     * total_discarded keeps track of the total
     * number of bytes discarded so far.
     * 
     * discard_size is the size of the discard *
     * buffer. Ideally we will only have to 
     * decompress (on average) spacing / 2 bytes 
     * before reaching the seek location, but this 
     * isn't a guarantee, so we allocate more to
     * reduce the number of reads that are required.
     */
    uint8_t *discard         = NULL; 
    uint64_t to_discard      = 0;
    uint64_t total_discarded = 0;
    uint64_t discard_size    = index->spacing * 4;

    if (len == 0)         return 0;
    if (len >  INT64_MAX) goto fail;
    
    zran_log("zran_read(%llu)\n", len);

    /* 
     * Search for the index point that 
     * corresponds to our current seek 
     * location in the uncompressed 
     * data stream. 
     */
    ret = _zran_get_point_with_expand(index,
                                      index->uncmp_seek_offset,
                                      0,
                                      &start);

    if (ret < 0) goto fail;
    if (ret > 0) goto not_covered_by_index;

    /*
     * We have to start decompressing from 
     * the index point that preceeds the seek 
     * location, so we need to skip over bytes
     * until we get to that location. We use
     * the discard buffer to store those bytes. 
     */
    discard = malloc(discard_size);
    if (discard == NULL) {
        goto fail;
    }

    /* 
     * Inflate and discard data until we 
     * reach the current seek location 
     * into the uncompresesd data stream.
     */
    cmp_offset      = start->cmp_offset;
    uncmp_offset    = start->uncmp_offset;
    first_inflate   = 1;
    total_discarded = 0;
    
    while (uncmp_offset < index->uncmp_seek_offset) {

        /* 
         * On the first call to _zran_inflate,
         * we tell it to initialise the z_stream,
         * and create a read buffer.
         */
        if (first_inflate) {
            first_inflate = 0;
            inflate_flags = (ZRAN_INFLATE_INIT_Z_STREAM         |
                             ZRAN_INFLATE_INIT_READBUF          |
                             ZRAN_INFLATE_CLEAR_READBUF_OFFSETS |
                             ZRAN_INFLATE_USE_OFFSET);
        }
        /*
         * On subsequent calls, we just tell 
         * _zran_inflate to continue where 
         * it left off.
         */
        else {
            inflate_flags = 0;
        }

        /* 
         * Don't read past the uncompressed seek 
         * location - at this point, we will need 
         * to stop discarding bytes, and start 
         * fulfilling the read request.
         *
         * TODO to_discard >= 2**32
         */
        to_discard = index->uncmp_seek_offset - uncmp_offset;
        if (to_discard > discard_size)
            to_discard = discard_size;

        zran_log("Discarding %llu bytes (%llu < %llu)\n",
                 to_discard,
                 uncmp_offset,
                 index->uncmp_seek_offset);

        ret = _zran_inflate(index,
                            &strm,
                            cmp_offset,
                            inflate_flags,
                            &bytes_consumed,
                            &bytes_output,
                            to_discard,
                            discard);

        /* 
         * _zran_inflate should return 0 if 
         * it runs out of output space (which 
         * is ok), or it has read enough bytes
         * (which is perfect). Any other 
         * return code means that something 
         * has gone wrong.
         */ 
        if (ret != ZRAN_INFLATE_OUTPUT_FULL &&
            ret != ZRAN_INFLATE_EOF         &&
            ret != ZRAN_INFLATE_OK)
            goto fail;

        cmp_offset      += bytes_consumed;
        uncmp_offset    += bytes_output;
        total_discarded += bytes_output;
    }

    /*
     * Sanity check - we should be at the 
     * correct location in the uncompressed 
     * stream. 
     *
     * TODO What happens here if we are at EOF?
     */
    if (uncmp_offset != index->uncmp_seek_offset)
        goto fail;

    zran_log("Discarded %llu bytes, ready to "
             "read from %llu (== %llu)\n",
             total_discarded,
             uncmp_offset,
             index->uncmp_seek_offset);

    /*
     * At this point, we are ready to inflate 
     * from the uncompressed seek location. 
     */
    // 
    // TODO Support len >= 2**32
    // 
    total_read = 0;
    while (total_read < len) {

        /*
         * If we started at the correct location, 
         * the discard loop above will not have 
         * executed, and _zran_inflate will not 
         * have initialised itself. So we repeat 
         * the flag control stuff here.
         */
        if (first_inflate) {
            first_inflate = 0;
            inflate_flags = (ZRAN_INFLATE_INIT_Z_STREAM         |
                             ZRAN_INFLATE_INIT_READBUF          |
                             ZRAN_INFLATE_CLEAR_READBUF_OFFSETS |
                             ZRAN_INFLATE_USE_OFFSET);
        }
        else {
            inflate_flags = 0;
        } 

        ret =_zran_inflate(index,
                           &strm,
                           cmp_offset,
                           inflate_flags,
                           &bytes_consumed,
                           &bytes_output,
                           len,
                           buf);

        cmp_offset   += bytes_consumed;
        uncmp_offset += bytes_output;
        total_read   += bytes_output;

        if (ret == ZRAN_INFLATE_OUTPUT_FULL ||
            ret == ZRAN_INFLATE_EOF)
            break;
        else if (ret != ZRAN_INFLATE_OK)
            goto fail;

        zran_log("Read %u bytes (%llu / %llu)\n",
                 bytes_output,
                 total_read,
                 len); 
    }

    /* 
     * A final call to _zran_inflate,  
     * to clean up memory
     */
    ret = _zran_inflate(index,
                        &strm,
                        0,
                        (ZRAN_INFLATE_CLEAR_READBUF_OFFSETS |
                         ZRAN_INFLATE_FREE_Z_STREAM         |
                         ZRAN_INFLATE_FREE_READBUF),
                        &bytes_consumed,
                        &bytes_output,
                        0,
                        discard);

    if (ret != ZRAN_INFLATE_OK && ret != ZRAN_INFLATE_EOF) {
        goto fail;
    }

    /* 
     * Update the current uncompressed 
     * seek position.
     */
    index->uncmp_seek_offset += total_read;

    zran_log("Read succeeded - %llu bytes read [compressed offset: %ld]\n",
             total_read,
             ftell(index->fd));

    free(discard);
    
    return total_read;

not_covered_by_index:
    return -1;

fail:

    if (discard != NULL)
        free(discard);

    return -2;
}
