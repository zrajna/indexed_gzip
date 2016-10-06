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
 * Searches for the zran_point corresponding to the given offset, which may be
 * specified as being relative to the start of the compressed data, or the
 * uncompressed data.
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
    
    uint8_t      *data          /* Pointer to zran_index_t->window_size bytes
                                   of uncompressed data preceeding this index
                                   point. */
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
    index->fd                = fd;
    index->flags             = flags;
    index->compressed_size   = compressed_size;
    index->spacing           = spacing;
    index->window_size       = window_size;
    index->readbuf_size      = readbuf_size;
    index->npoints           = 0;
    index->size              = 8;
    index->uncmp_seek_offset = 0;
    index->list              = point_list;
    
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
     * covers the specified offset. If there;s
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

        if (_zran_expand_index(index, expand) != 0) {
            goto fail;
        }

        result = _zran_get_point_at(index, offset, compressed, point); 
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
    if (result == 0 && point == NULL) {
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
                    uint8_t       *data) {

    zran_log("_zran_add_point(%i, c=%lld + %i, u=%lld)\n",
             index->npoints,
             cmp_offset,
             bits > 0,
             uncmp_offset);

    uint8_t      *point_data = NULL;
    zran_point_t *next       = NULL;

    /* if list is full, make it bigger */
    if (index->npoints == index->size) {
        if (_zran_expand_point_list(index) != 0) {
            goto fail;
        }
    }

    /* 
     * Allocate memory to store the uncompressed 
     * data associated with this point.
     */
    point_data = calloc(1, index->window_size);
    if (point_data == NULL)
        goto fail;

    next               = &(index->list[index->npoints]);
    next->bits         = bits;
    next->cmp_offset   = cmp_offset;
    next->uncmp_offset = uncmp_offset;
    next->data         = point_data;

    /* 
     * The uncompressed data may not start at 
     * the beginning of the data pointer, but 
     * rather from an arbitrary point. So we 
     * copy the first block of uncompressed
     * data.
     */
    if (data_offset)
        memcpy(point_data,
               data + data_offset,
               index->window_size - data_offset);

    /* And then copy the remainder. */
    if (data_offset < index->window_size)
        memcpy(point_data + index->window_size - data_offset,
               data,
               data_offset);

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
    windowBits       = (int)round(log10(index->window_size) / log10(2)); 
    stream->zalloc   = Z_NULL;
    stream->zfree    = Z_NULL;
    stream->opaque   = Z_NULL;
    stream->avail_in = 0;
    stream->next_in  = Z_NULL;

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
        if (inflateSetDictionary(stream,
                                 point->data,
                                 index->window_size) != Z_OK)
            goto fail; 
    }

    return 0;

fail:
    return -1;
}


/*
 * Expands the index to encompass the compressed offset specified by 'until'.
 */
int _zran_expand_index(zran_index_t *index, uint64_t until)
{

    /* Used to store and check return values. */
    int            ret;

    /* 
     * Counters to keep track of where we are 
     * in both the compressed and uncompressed 
     * streams.
     */
    uint64_t       cmp_offset;
    uint64_t       uncmp_offset;

    /* 
     * A reference to the last point in 
     * the index. This is where we need 
     * to start decompressing data from 
     * before we can add more index points.
     */
    zran_point_t  *start = NULL;

    /* 
     * Uncompresse data offset of the 
     * most recent point that was added
     * to the index.
     */
    uint64_t       last_uncmp_offset;

    /*
     * Tally of the number of points 
     * we have expanded the index by.
     */
    uint64_t       points_created = 0;

    /* Zlib stream struct */
    z_stream       strm;

    /* Buffers to store compresed and uncompressed data */
    uint8_t       *input  = NULL;
    uint8_t       *window = NULL;

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

    /* Otherwise, we start at the beginning */
    else {
        start = NULL;
    }

    /* 
     * Allocate memory for the data 
     * buffers, bail on failure.
     */
    input = calloc(1, index->readbuf_size);
    if (input == NULL)
        goto fail;

    window = calloc(1, index->window_size);
    if (window == NULL)
        goto fail;

    zran_log("_zran_expand_index(%llu)\n", until);

    /*
     * Force some data to be read
     */
    if (until == 0) {
      until += index->readbuf_size;
    }

    /* Initialise the zlib struct for inflation */
    if (_zran_init_zlib_inflate(index, &strm, start) != 0)
        goto fail;

    /*
     * We start from the compressed data location
     * that correspods to the second to last point 
     * in the index.
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
     * Continue until the specified offset 
     * (stored in until), or the end of the 
     * stream is reached.  We initialise
     * avail_out to 0, in case we are at 
     * the beginning of the compressed file,
     * in which case inflate() will process
     * the file header.
     */
    strm.avail_out = 0;

    do {

        /* Read a block of compressed data */
        ret = fread(input, 1, index->readbuf_size, index->fd);

        if (ret <= 0)          goto fail;
        if (ferror(index->fd)) goto fail;

        /* 
         * Process the block of compressed 
         * data that we just read in.
         */
        strm.avail_in = ret;
        strm.next_in  = input;
        
        do {

            /* 
             * Reset the uncompressed 
             * buffer if necessary.
             */
            if (strm.avail_out == 0) {
                strm.avail_out = index->window_size;
                strm.next_out  = window;
            }

            /* 
             * inflate until out of input, output, 
             * or at end of block - update the 
             * total input and output counters
             */
            cmp_offset   += strm.avail_in;
            uncmp_offset += strm.avail_out;

            /* 
             * Inflate the block - the decompressed 
             * data is stored in the window buffer.
             * Z_BLOCK tells zlib to stop inflating
             * at a compression block boundary - our
             * file indices need to be located at 
             * these boundaries.
             */
            ret = inflate(&strm, Z_BLOCK);
            if      (ret == Z_STREAM_END) break;
            else if (ret != Z_OK)         goto fail;

            /*
             * Adjust our offsets according to 
             * what was actually decompressed.
             */
            cmp_offset   -= strm.avail_in;
            uncmp_offset -= strm.avail_out;

            /* 
             * If we're at the begininng of the file (uncmp_offset == 0),
             * or at the end of a deflate block, and index->spacing bytes
             * have passed since the last index point that was created,
             * we'll create a new index point at this location.
             */
            if ( (strm.data_type & 128) &&
                !(strm.data_type & 64)  &&
                (uncmp_offset == 0 ||
                 uncmp_offset - last_uncmp_offset > index->spacing)) {

                if (_zran_add_point(index,
                                    strm.data_type & 7,
                                    cmp_offset,
                                    uncmp_offset,
                                    index->window_size - strm.avail_out,
                                    window) != 0) {
                    goto fail;
                }

                points_created   += 1;
                last_uncmp_offset = uncmp_offset;
            }

            /* 
             * Break when we've expanded the index 
             * far enough. In some data there may 
             * be a long distance between deflate 
             * boundaries (longer than the desired 
             * index point spacing), so ensure that 
             * we have created at least one new 
             * index point (hence the points_created 
             * check). 
             */
            if (points_created > 0 && cmp_offset >= until) break; 

        } while (strm.avail_in != 0);

    /*
     * Don't finish until we're at the end 
     * of the stream, or we've expanded the 
     * index far enough (and have created 
     * at least one new index point).
     */
    } while (ret != Z_STREAM_END && (cmp_offset < until || points_created == 0));

    /*
     * The index may have over-allocated 
     * space for storing index points, so 
     * here we free the unused memory.
     */
    if (_zran_free_unused(index) != 0) {
        goto fail;
    }
    
    /* 
     * Tell zlib we're finished, free
     * the data buffers, and return 
     */
    inflateEnd(&strm);
    free(window);
    free(input);
    return 0;

fail:
    inflateEnd(&strm);
    if (window != NULL) free(window);
    if (input  != NULL) free(input);
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
    long          ret;


    /* Keeps track of the total number of bytes read */
    uint64_t      total_read = 0;

    /* 
     * Counters keeping track of the 
     * current location in both the 
     * compressed and uncompressed
     * streams.
     */
    off_t         uncmp_offset;
    off_t         cmp_offset;

    /* 
     * Flag used to control skipping 
     * over uncompressed data until 
     * we get to the right point.
     */
    int           skip;

    /* 
     * Zlib stream struct and read/decompress buffers.
     *
     * z_stream.avail_out might not be able 
     * to store numbers as big as what len
     * can, so we may have to chunk up the 
     * calls. 
     *
     * I'm assuming here is that 
     * strm.avail_out is unsigned.
     */
    z_stream      strm;
    zran_point_t *point;
    uint64_t      avail_out_max = pow(2, (sizeof(strm.avail_out) * 8)) - 1;
    uint8_t      *input         = NULL;
    uint8_t      *discard       = NULL;

    if (len == 0)         return 0;
    if (len >  INT64_MAX) goto fail;
    
    zran_log("zran_read(%zu)\n", len);

    /* 
     * Buffer to store compressed 
     * data from the file.
     */
    input = calloc(1, index->readbuf_size);
    if (input == NULL)
        goto fail; 

    /* 
     * We have to decompress from the beginning
     * of the index point corresponding to the 
     * offset, throwing away the data until
     * we reach the correct point in the 
     * uncompressed stream.
     */
    discard = calloc(1, index->window_size);
    if (discard == NULL)
        goto fail;

    /* 
     * Search for the index 
     * point that corresponds to 
     * our current location in the 
     * uncompressed data stream (minus
     * the number of bytes we need to 
     * read in).
     */
    uncmp_offset = index->uncmp_seek_offset - len;
    if (uncmp_offset < 0)
        uncmp_offset = 0;
    
    ret = _zran_get_point_with_expand(index,
                                      uncmp_offset,
                                      0,
                                      &point);

    if (ret < 0) goto fail;
    if (ret > 0) goto not_covered_by_index;

    /* Initialise zlib for inflation */
    if (_zran_init_zlib_inflate(index, &strm, point) != 0)
        goto fail;

    /* 
     * Read and decompress data from the 
     * file, discarding the uncompressed 
     * bytes, until we get to the end of the 
     * file, or to the requested uncompressed 
     * offset. The uncmp_offset is adjusted to
     * be relative to the starting index point.
     */
    skip          = 1;
    cmp_offset    = point->cmp_offset ;
    uncmp_offset  = index->uncmp_seek_offset - point->uncmp_offset;
    strm.avail_in = 0;

    zran_log("Attempting to read %li bytes from "
             "uncompressed offset %llu (starting "
             "from compressed offset %llu, )\n",
             len, index->uncmp_seek_offset, point->cmp_offset);

    do {
        /*
         * We've skipped over enough 
         * uncompressed bytes, and are 
         * now able to read/decompress
         * the requested bytes.
         */
        if (uncmp_offset == 0 && skip) {

            if (len > avail_out_max) strm.avail_out = avail_out_max;
            else                     strm.avail_out = len;

            strm.next_out  = buf;
            skip           = 0; 
        }

        /* 
         * We haven't yet reached the 
         * requested uncompressed offset -
         * skip over window_size uncompresed 
         * bytes at a time until we get 
         * to the right location.
         */
        else if (uncmp_offset > index->window_size) {
            strm.avail_out = index->window_size;
            strm.next_out  = discard;
            uncmp_offset  -= index->window_size;
        }

        /*
         * We're almost at the right location
         * (within window_size bytes of it).
         * This will be the last blcok of bytes
         * that we decompress and discard, before
         * we can start reading in the requested
         * data.
         */
        else if (uncmp_offset != 0) {

            // TODO Will uncmp_offset ever be > 2**32?
            strm.avail_out = (unsigned)uncmp_offset;
            strm.next_out  = discard;
            uncmp_offset   = 0;
        }

        /* 
         * Read and decompress a block of 
         * data until end of file. or our 
         * decompressed data buffer is full.
         */
        do {
            
            /* 
             * Read a block of compressed 
             * bytes from the file 
             */
            if (strm.avail_in == 0) {
                
                ret = fread(input, 1, index->readbuf_size, index->fd);

                if (ferror(index->fd)) goto fail;
                if (ret == 0)          goto fail;

                strm.avail_in = ret;
                strm.next_in  = input;
            }
            
            /* 
             * Decompress the block 
             * that we just read in.
             */
            ret = inflate(&strm, Z_NO_FLUSH);

            /* End of file or something went wrong */
            if      (ret == Z_STREAM_END) break;
            else if (ret != Z_OK)         goto fail;

            /* 
             * If the read request was for more than 2**32 
             * bytes, we need to adjust z_stream.avail_out 
             * (which is uint32), and continue reading.
             */
            if (!skip && strm.avail_out == 0 && len > avail_out_max) {

                total_read += avail_out_max;
                len        -= avail_out_max;

                if (len > avail_out_max) strm.avail_out = avail_out_max;
                else                     strm.avail_out = len;
            }

        /* 
         * Break the read/decompress loop when 
         * our decompress buffer is full. 
         */
        } while (strm.avail_out != 0);

        /* 
         * End of file - break the main loop
         */
        if (ret == Z_STREAM_END)
            break;

    /* 
     * Keep looping until we're at 
     * the requested location in the 
     * uncompressed data stream.
     */
    } while (skip);

    /* 
     * Clean up zlib
     */
    inflateEnd(&strm);

    /* 
     * How many uncompressed bytes 
     * did we actually read?
     */
    if (skip) len  = 0;
    else      len -= strm.avail_out;

    total_read += len;

    /* 
     * Update the current uncompressed 
     * seek position.
     */
    index->uncmp_seek_offset += total_read;

    zran_log("Read succeeded - %llu bytes read [compressed offset: %ld]\n",
             total_read,
             ftell(index->fd));

    free(input);
    free(discard);
    return total_read;


not_covered_by_index:

    if (input   != NULL) free(input);
    if (discard != NULL) free(discard);
    
    inflateEnd(&strm);
    return -1;

fail:

    if (input   != NULL) free(input);
    if (discard != NULL) free(discard);
    
    inflateEnd(&strm);
    return -2;
}
