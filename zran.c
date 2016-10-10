/*
 * zran.c - indexed access to gzip files.
 * 
 * See zran.h for documentation.
 *
 * Most of this code has been adapted from the zran example, written by Mark
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
int ZRAN_INFLATE_OK             = 0;

/* 
 * _zran_inflate input flags. 
 * Bit position, as a power of 2 
 */
int ZRAN_INFLATE_INIT_Z_STREAM = 1;
int ZRAN_INFLATE_FREE_Z_STREAM = 2;
int ZRAN_INFLATE_INIT_READ_BUF = 4;
int ZRAN_INFLATE_FREE_READ_BUF = 8;
int ZRAN_INFLATE_USE_OFFSET    = 16;
int ZRAN_INFLATE_STOP_AT_BLOCK = 32;

/* Macros used by _zran_inflate for testing flags. */
#define inflate_stop_at_block(flags) ((flags & ZRAN_INFLATE_STOP_AT_BLOCK) > 0)
#define inflate_use_offset(   flags) ((flags & ZRAN_INFLATE_USE_OFFSET)    > 0)
#define inflate_init_stream(  flags) ((flags & ZRAN_INFLATE_INIT_Z_STREAM) > 0)
#define inflate_free_stream(  flags) ((flags & ZRAN_INFLATE_FREE_Z_STREAM) > 0)
#define inflate_init_read_buf(flags) ((flags & ZRAN_INFLATE_INIT_READ_BUF) > 0)
#define inflate_free_read_buf(flags) ((flags & ZRAN_INFLATE_FREE_READ_BUF) > 0)

/*
 * TODO Make this documentation reflect reality
 * 
 * Inflate (decompress) the specified number of bytes, or until the next
 * Z_BLOCK/Z_STREAM_END is reached.
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
 *   - ZRAN_INFLATE_INIT_READ_BUF
 *   - ZRAN_INFLATE_FREE_READ_BUF
 *   - ZRAN_INFLATE_USE_OFFSET
 *   - ZRAN_INFLATE_STOP_AT_BLOCK
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
    
    uint32_t      len,            /* Maximum number of bytes to inflate. */
    
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
    index->readbuf_offset       = 0;
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
     */
    if (last == NULL)
        estimate = offset * 2.0;

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
     * associated with this point.
     */
    if (uncmp_offset > 0) {
        point_data = calloc(1, index->window_size);
        if (point_data == NULL)
            goto fail;
    }
    else {
        point_data = NULL;
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
    
    /* 
     * Calculate the  base2 logarithm 
     * of the window size - it is needed 
     * to initialise zlib inflation.
     * And initialise the zlib struct.
     */
    windowBits        = index->log_window_size; 
    stream->zalloc    = Z_NULL;
    stream->zfree     = Z_NULL;
    stream->opaque    = Z_NULL;
    stream->avail_in  = 0;
    stream->avail_out = 0;
    stream->next_in   = Z_NULL;

    /*
     * Seek to the required location in the compressed 
     * data stream. 
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
    size_t         f_ret;
    int            z_ret;
    int            return_val = 0;

    /* 
     * Offsets into the compressed 
     * and uncompressed data streams. 
     */
    uint64_t       cmp_offset;
    uint64_t       uncmp_offset;

    /* 
     * Index point to start from 
     * (if ZRAN_INFLATE_USE_OFFSET 
     * is active).
     */
    zran_point_t  *start = NULL;

    if (len == 0)
        return 0;

    zran_log("_zran_inflate(%llu, block=%u, use_offset=%u, init_stream=%u,\n"
             "              free_stream=%u, init_readbuf=%u, free_readbuf=%u\n"
             "              nbytes=%u)\n",
             offset,
             inflate_stop_at_block(flags),
             inflate_use_offset(   flags),
             inflate_init_stream(  flags),
             inflate_free_stream(  flags),
             inflate_init_read_buf(flags),
             inflate_free_read_buf(flags), 
             len);

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

        /*
         * If a non-zero offset has been specified, 
         * search the index to see if we can start 
         * inflating from a known location.
         */
        if (offset > 0) {

            /*
             * In order to successfully decompress data from the
             * current uncompressed seek location, we need to
             * start decompressing from the index point which
             * preceeds it.
             *
             * TODO We may need to start decompressing from the
             *      index point before the one which preceeds
             *      the requested offset. Needs testing.
             */
            z_ret = _zran_get_point_at(index, offset, 1, &start);

            if (z_ret < 0) return ZRAN_INFLATE_ERROR;
            if (z_ret > 0) return ZRAN_INFLATE_NOT_COVERED;

            zran_log("Point for offset %llu: c=%llu, u=%llu\n",
                     offset, start->cmp_offset, start->uncmp_offset);
        }

        /*
         * No index point - we need to start 
         * from the beginning of the file.
         */
        if (start == NULL) {

            cmp_offset   = 0;
            uncmp_offset = 0;
        }

        /*
         * Or we start from an index point 
         * which covers the requested offset
         */
        else {
            cmp_offset   = start->cmp_offset;
            uncmp_offset = start->uncmp_offset;
        }
    }
    
    /*
     * If ZRAN_INFLATE_USE_OFFSET is not active, 
     * we initialise from offsets which were 
     * stored on the last call to _zran_inflate..
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
     * If ZRAN_INFLATE_INIT_READ_BUF, 
     * allocate memory for reading 
     * compressed data from the file
     */
    if (inflate_init_read_buf(flags)) {
        index->readbuf_offset = 0;
        index->readbuf_end    = 0;
        index->readbuf        = calloc(1, index->readbuf_size);
        if (index->readbuf == NULL)
            goto fail;
    }

    /*
     * Otherwise, assume that index->readbuf 
     * and index->readbuf_size have been set 
     * on a previous call, and use them.
     */
    else {
        zran_log("Setting avail_in to %u - %u = %u\n",
                            index->readbuf_end,  index->readbuf_offset,
                 (uint32_t)(index->readbuf_end - index->readbuf_offset));
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
     * Don't finish until we're at the end of
     * the file, or we've read enough bytes.
     */
    while (strm->avail_out > 0) {

        /* We need to read in more data */
        if (strm->avail_in == 0) {

            if (feof(index->fd)) {
                return_val = ZRAN_INFLATE_EOF;
                break;
            }
            
            /* Read a block of compressed data */
            zran_log("Reading from file %llu [ == %llu?]\n",
                     ftello(index->fd),
                     cmp_offset);
            f_ret = fread(index->readbuf, 1, index->readbuf_size, index->fd);

            if (ferror(index->fd)) goto fail;
            if (f_ret == 0)        goto fail;

            index->readbuf_end = f_ret;

            zran_log("Read %lu bytes from file [c=%llu, u=%llu]\n",
                     f_ret, cmp_offset, uncmp_offset);

            zran_log("Block read: [%02x %02x %02x %02x ...]\n",
                     index->readbuf[0],
                     index->readbuf[1],
                     index->readbuf[2],
                     index->readbuf[3]);

            /*
             * Tell zlib about the block 
             * of compressed data that we
             * just read in.
             */

            /*
             * f_ret (size_t) should not overflow avail_in 
             * (unsigned int) because, in this situation, 
             * f_ret should never have a value greater than 
             * index->readbuf_size (which is uint32_t).
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
                total_consumed += z_ret;
            }

            /* 
             * Optimistically update offsets - 
             * we will adjust them after the 
             * inflate call.
             */
            cmp_offset   += strm->avail_in;
            uncmp_offset += strm->avail_out;

            zran_log("Before inflate - avail_in=%u, avail_out=%u\n",
                     strm->avail_in,
                     strm->avail_out);

            zran_log("Read buf location: (%u - %u): %u\n",
                     index->readbuf_end,  strm->avail_in,
                     index->readbuf_end - strm->avail_in);
            zran_log("Read buffer: [%02x %02x %02x %02x ...]\n",
                     strm->next_in[0],
                     strm->next_in[1],
                     strm->next_in[2],
                     strm->next_in[3]);

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
                     strm->avail_in,
                     strm->avail_out); 

            /*
             * Adjust our offsets according to what
             * was actually consumed/decompressed.
             */
            cmp_offset   -= strm->avail_in;
            uncmp_offset -= strm->avail_out;

            /* End of file */
            if (feof(index->fd) && strm->avail_in <= 8) {
                zran_log("End of file, stopping inflation\n");
                return_val = ZRAN_INFLATE_EOF;
                break;
            }

            /*
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
             * So we only stop when at Z_STREAM_END, 
             * *and* we are at end of file, or 
             * ZRAN_INFLATE_STOP_AT_BLOCK is active.
             *
             * If at a new stream, we re-initialise
             * inflation on the next loop iteration.
             */
            if (z_ret == Z_STREAM_END ||
                ((strm->data_type & 128) && !(strm->data_type & 64))) {

                /* 
                 * TODO  I think you can simplify the logic 
                 *       here (combine it with the z_ret == 
                 *       Z_OK clause below). Maybe you can 
                 *       have the stop_at_block/ checks at 
                 *       the bottom of the outer loop?
                 */ 
                if (inflate_stop_at_block(flags)) {
                    zran_log("End of stream, stopping inflation "
                             "(treating as block boundary)\n");
                    return_val = ZRAN_INFLATE_BLOCK_BOUNDARY;
                    break;
                }
            }

            /* 
             * No room left to store decompressed data
             */
            else if (z_ret == Z_BUF_ERROR) {
                zran_log("Output buffer full - stopping inflation\n");
                return_val = ZRAN_INFLATE_OUTPUT_FULL;
                break;
            }

            else if (z_ret != Z_OK) {
                zran_log("inflate failed (code: %i, msg: %s)\n",
                         z_ret, strm->msg);
                goto fail;
            }
            
            if (strm->avail_out == 0) {
                zran_log("Output buffer full - stopping inflation\n");
                return_val = ZRAN_INFLATE_OUTPUT_FULL;
                break;
            }

            /* 
             * If we used Z_BLOCK, and inflate 
             * returned Z_OK, it means we've 
             * found a block boundary.
             */
            if (inflate_stop_at_block(flags) && strm->avail_in > 0)
                return_val = ZRAN_INFLATE_BLOCK_BOUNDARY;

            if (return_val != 0)
                break;
        }

        /*
         * If ZRAN_INFLATE_STOP_AT_BLOCK is 
         * active, and return_val has been set 
         * to non-0, it means we've found a 
         * block.
         */
        if (return_val != 0)
            break;
    }

    /*
     * If ZRAN_INFLATE_MANAGE_Z_STREAM is 
     * active, clear input buffer memory.
     */

    if (inflate_free_read_buf(flags)) {
        free(index->readbuf);
        index->readbuf        = NULL;
        index->readbuf_offset = 0;
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
    if (inflate_use_offset(flags)) {
        if (start == NULL) *total_consumed = cmp_offset;
        else               *total_consumed = cmp_offset - start->cmp_offset;

        if (start == NULL) *total_output = uncmp_offset;
        else               *total_output = uncmp_offset - start->uncmp_offset;
    }
    else {
        *total_consumed = cmp_offset   - index->inflate_cmp_offset;
        *total_output   = uncmp_offset - index->inflate_uncmp_offset;
    }

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

    /* Return 0 or ZRAN_INFLATE_BLOCK_BOUNDARY */
    return return_val;

fail:
    if (index->readbuf != NULL) {
        free(index->readbuf);
        index->readbuf        = NULL;
        index->readbuf_offset = 0;
    }
    
    return ZRAN_INFLATE_ERROR;
}


/*
 * Expands the index to encompass the 
 * compressed offset specified by 'until'.
 */
int _zran_expand_index(zran_index_t *index, uint64_t until)
{

    /* Used to store and check return values. */
    int            z_ret;
    
    /*
     * Number of bytes read/decompressed 
     * on each call to _zran_inflate.
     */
    uint32_t       bytes_consumed;
    uint32_t       bytes_output;

    /* 
     * Size of the buffer used to store 
     * uncompressed data. Ideally, we only
     * want to decompress index->spacing 
     * bytes before creating a new index
     * point. But we may have to decompress 
     * more than this before a suitable
     * location (a block/stream boundary) 
     * is found, so we allocate more space.
     */

    /* Buffer to store uncompressed data */
    uint8_t       *data = NULL;
 
    uint32_t       data_size   = index->spacing * 4;
    /* 
     * Current position in the data buffer.
     */
    uint32_t       data_offset = 0;

    /*
     * _zran_inflate control flags. We need 
     * to use different flags on the first 
     * call - first_inflate is used to track 
     * this.
     */
    uint16_t       inflate_flags;
    uint8_t        first_inflate = 1; 

    /* 
     * Counters to keep track of where we are 
     * in both the compressed and uncompressed 
     * streams. last_uncmp_offset is the 
     * uncompress  data offset of the most 
     * recent point that was added to the index
     * in this call to _zran_expand_index.
     */
    uint64_t       cmp_offset;
    uint64_t       uncmp_offset;
    uint64_t       last_uncmp_offset;
 
    /* 
     * A reference to the last point in 
     * the index. This is where we need 
     * to start decompressing data from 
     * before we can add more index points.
     */
    zran_point_t  *start = NULL;

    /*
     * Tally of the number of points 
     * we have expanded the index by
     * on this call.
     */
    uint64_t       points_created = 0;

    /* Zlib stream struct */
    z_stream       strm;

    /*
     * In order to create a new index 
     * point, we need to start reading 
     * at the last index point, so that 
     * we read enough data to initialise 
     * the inflation.
     */
    if (index->npoints > 1) {
        
        start = &(index->list[index->npoints - 1]);

        /*
         * The index already covers 
         * the requested offset
         */ 
        if (until <= start->cmp_offset)
            return 0;
    }

    /* 
     * Otherwise, we start at the 
     * beginning of the file. 
     */
    else {
        start = NULL;
    }

    /* Allocate memory for the data buffer. */
    data = calloc(1, data_size);
    if (data == NULL)
        goto fail;

    zran_log("_zran_expand_index(%llu)\n", until);

    /*
     * If the caller passed until == 0, 
     * we force some data to be read
     */
    if (until == 0) {
      until = index->spacing;
    }

    /*
     * We start from the last 
     * point in the index.
     */
    if (start != NULL) {

        cmp_offset        = start->cmp_offset;
        uncmp_offset      = start->uncmp_offset;
        last_uncmp_offset = uncmp_offset;
    }

    /* 
     * Or the beginning of the 
     * file, if the index is empty. 
     */
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
     * because in some data there may be a long 
     * distance between deflate boundaries (longer 
     * than the desired index point spacing).
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
            inflate_flags = (ZRAN_INFLATE_INIT_Z_STREAM |
                             ZRAN_INFLATE_INIT_READ_BUF |
                             ZRAN_INFLATE_USE_OFFSET    | 
                             ZRAN_INFLATE_STOP_AT_BLOCK);
        }

        /*
         * On subsequent calls, we tell _zran_inflate
         * to just continue where it left off on the
         * previous call. We will manually clear up
         * the read buffer and z_stream struct at the 
         * end.
         */
        else {
            inflate_flags = ZRAN_INFLATE_STOP_AT_BLOCK;
        }

        zran_log("Searching for next block boundary\n"
                 "       c=%llu, u=%llu,\n"
                 "       data_offset=%u, data_space=%u\n",
                 cmp_offset, uncmp_offset, data_offset,
                 data_size - data_offset);
        
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
         * Has the output buffer been 
         * filled, or eof been reached?
         * 
         */
        if      (z_ret == ZRAN_INFLATE_OUTPUT_FULL) continue;
        else if (z_ret == ZRAN_INFLATE_EOF)         break;

        /* 
         * If z_ret != ZRAN_INFLATE_BLOCK_BOUNDARY, 
         * something has gone wrong.
         */
        else if (z_ret != ZRAN_INFLATE_BLOCK_BOUNDARY) {
            zran_log("_zran_inflate did not find a block boundary (ret=%i)!\n",
                     z_ret);
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
     * The index may have over-allocated 
     * space for storing index points, so 
     * here we free the unused memory.
     */
    if (_zran_free_unused(index) != 0) {
        goto fail;
    }

    /* 
     * Clear the z_stream struct 
     * and file buffer.
     */
    if (inflateEnd(&strm) != Z_OK) {
        goto fail;
    }

    if (index->readbuf != NULL) {
        free(index->readbuf);
        index->readbuf        = NULL;
        index->readbuf_offset = 0;
    } 
    
    free(data);
    return 0;

fail:
    if (data != NULL)
        free(data);

    if (index->readbuf != NULL) {
        free(index->readbuf);
        index->readbuf        = NULL;
        index->readbuf_offset = 0;
    }
    
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
long zran_tell(zran_index_t *index)
{
  
    return index->uncmp_seek_offset;
}


/* Read len bytes from the uncompressed data stream, storing them in buf. */
int64_t zran_read(zran_index_t *index,
                  void         *buf,
                  uint64_t      len)
{

    /* Used to store/check return values. */
    int           ret;

    uint32_t      bytes_consumed;
    uint32_t      bytes_output;

    uint16_t      inflate_flags;
    uint8_t       first_inflate;


    /* Keeps track of the total number of bytes read */
    uint64_t      total_read;

    /* 
     * Counters keeping track of the 
     * current location in both the 
     * compressed and uncompressed
     * streams.
     */
    off_t         uncmp_offset;
    off_t         cmp_offset;

    /* 
     * Zlib stream struct and starting point.
     */
    z_stream      strm;
    zran_point_t *start = NULL;

    /*
     * Memory used to store bytes that we skip 
     * over before reaching the appropriate 
     * point in the uncompressed data stream.
     * Ideally we will only have to decompress 
     * (on average) spacing / 2 bytes before
     * reaching the seek location, but this 
     * isn't a guarantee, so we allocate more
     * to reduce the number of reads that are 
     * required.
     */
    uint64_t      to_discard      = 0;
    uint64_t      total_discarded = 0;
    uint64_t      discard_size    = index->spacing * 4;
    uint8_t      *discard         = NULL;

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
     * this buffer to store those bytes. 
     */
    discard = malloc(discard_size);
    if (discard == NULL) {
        goto fail;
    }

    zran_log("Attempting to read %llu bytes from "
             "uncompressed offset %llu (starting "
             "from c=%llu, u=%llu)\n",
             len,
             index->uncmp_seek_offset,
             start->cmp_offset,
             start->uncmp_offset);

    /* 
     * Inflate and discard data until we 
     * reach the current seek location 
     * into the uncompresesd data stream.
     */
    cmp_offset     = start->cmp_offset;
    uncmp_offset    = start->uncmp_offset;
    first_inflate   = 1;
    total_discarded = 0;
    
    while (uncmp_offset < index->uncmp_seek_offset) {

        if (first_inflate) {
            first_inflate = 0;
            inflate_flags = (ZRAN_INFLATE_INIT_Z_STREAM |
                             ZRAN_INFLATE_INIT_READ_BUF |
                             ZRAN_INFLATE_USE_OFFSET);
        }
        else {
            inflate_flags = 0;
        }

        /* 
         * Don't read past the uncompressed seek 
         * location - at this point, we will need 
         * to stop discarding bytes, and fulfil 
         * the read request.
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

        /* 
         * _zran_inflate should return 0 if 
         * it runs out of output space (which 
         * is ok), or it has read enough bytes
         * (which is perfect). Any other 
         * return code means that something has
         * gone wrong.
         */ 
        ret = _zran_inflate(index,
                            &strm,
                            cmp_offset,
                            inflate_flags,
                            &bytes_consumed,
                            &bytes_output,
                            to_discard,
                            discard);

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
    total_read = 0;
    while (total_read < len) {

        if (first_inflate) {
            first_inflate = 0;
            inflate_flags = (ZRAN_INFLATE_INIT_Z_STREAM |
                             ZRAN_INFLATE_INIT_READ_BUF |
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
     * Update the current uncompressed 
     * seek position.
     */
    index->uncmp_seek_offset += total_read;

    zran_log("Read succeeded - %llu bytes read [compressed offset: %ld]\n",
             total_read,
             ftell(index->fd));

    free(discard);

    if (index->readbuf != NULL) {
        free(index->readbuf);
        index->readbuf        = NULL;
        index->readbuf_offset = 0;
    }
    
    return total_read;


not_covered_by_index:
    return -1;

fail:

    if (discard != NULL)
        free(discard);

    if (index->readbuf != NULL) {
        free(index->readbuf);
        index->readbuf        = NULL;
        index->readbuf_offset = 0;
    } 
    return -2;
}
