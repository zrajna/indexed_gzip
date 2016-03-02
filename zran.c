/*
 *
 */
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "zlib.h"

#include "zran.h"


//#define ZRAN_VERBOSE


#ifdef ZRAN_VERBOSE
#define zran_log(...) fprintf(stderr, __VA_ARGS__)
#else
#define zran_log(...) 
#endif


/*
 * TODO If/when you add write capability, you'll need something like this, to
 *      mark a location in the index after which all index points should be
 *      discarded.
 * 
 * static int _zran_invalidate_index(zran_index_t *index, off_t from);
 */


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
 * If this index has been created with the ZRAN_AUTO_BUILD flag, and the
 * requested offset is not covered by the index, this function will call
 * _zran_expand_index to expand the index to to cover the requested offset.
 * 
 * Returns:
 *
 *   - 0 on success. 
 *
 *   - A negative number if something goes wrong.
 *
 *   - A positive number to indicate that an index covering the specified
 *     offset has not yet been created (unless the index has been created with
 *     the ZRAN_AUTO_BUILD flag).
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
 * Expands the index from its current end-point until the given offset (which
 * must be specified relative to the compressed data stream).
 *
 * If the offset is == 0, the index is expanded to cover the entire file
 * (i.e. it is fully built).
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
                                   are not byte-aligned, this is the number of
                                   bits in the compressed data, before the
                                   cmp_offset, where the point is located. */
    uint64_t      cmp_offset,   /* Offset into the compressed data. */
    uint64_t      uncmp_offset, /* Offset into the uncompressed data. */
    uint32_t      bytes_left,   /* Number of ... */
    uint8_t      *data          /**/
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

    zran_log("zran_init(%u, %u, %u)\n", spacing, window_size, readbuf_size);

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

/* (Re-)Builds the full index. */
int zran_build_index(zran_index_t *index)
{

    /* TODO zran_invalidate_index(index, 0); */

    return _zran_expand_index(index, 0);
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
     * TODO A bad compressed offset is easily checked 
     *      against the file size, but a bad uncompressed
     *      offset (i.e. one larger than the size of the 
     *      uncompressed data) cannot easily be checked.
     *     
     *      What happens when we pass a bad uncompressed
     *      offset in? This needs testing.
     */
    else {
        last      = &(index->list[index->npoints - 1]);
        uncmp_max = last->uncmp_offset + index->spacing * 1.1;
        cmp_max   = last->cmp_offset   + index->spacing;
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
     * 
     * TODO Use bsearch instead of this 
     *      linear search. All you have
     *      to do is define one (or two)
     *      point comparison functions.
     */
    prev = index->list;
    for (i = 1; i < index->npoints; i++) {
        
        curr = &(index->list[i]);

        if (compressed) {

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


uint64_t _zran_estimate_offset(
    zran_index_t *index,
    uint64_t      offset,
    uint8_t       compressed)
{

    zran_point_t *last;
    uint64_t      estimate;

    if (index->npoints == 0) last = NULL;
    else                     last = &(index->list[index->npoints - 1]);

    /*
     * We have no reference. At least one
     * index point needs to be created.
     */
    if (last == NULL)
        estimate = offset * 2.0;

    /*
     * I'm just assuming a roughly linear correspondence
     * between compressed/uncompressed. 
     */
    else if (compressed) {
        estimate = round(offset * ((float)last->uncmp_offset / last->cmp_offset));
    }
    else {
        estimate = round(offset * ((float)last->cmp_offset / last->uncmp_offset));
    }

    if (last != NULL)
        zran_log("Index extent: c=%lld, u=%lld\n",
                 last->cmp_offset,
                 last->uncmp_offset);

    zran_log("_zran_estimate_offset(%llu, %u) = %llu\n",
             offset,
             compressed,
             estimate); 

    return estimate;
}
                          


/* Add an entry to the access point list. */
int _zran_add_point(zran_index_t  *index,
                    uint8_t        bits,
                    uint64_t       cmp_offset,
                    uint64_t       uncmp_offset,
                    uint32_t       bytes_left,
                    uint8_t       *data) {

    zran_log("_zran_add_point(%i, c=%lld, u=%lld)\n",
             index->npoints,
             cmp_offset,
             uncmp_offset);

    uint8_t      *point_data = NULL;
    zran_point_t *next       = NULL;

    /* if list is full, make it bigger */
    if (index->npoints == index->size) {
        if (_zran_expand_point_list(index) != 0) {
            goto fail;
        }
    }

    point_data = calloc(1, index->window_size);
    if (point_data == NULL)
        goto fail;

    /* fill in entry and increment how many we have */
    next               = index->list + index->npoints;
    next->bits         = bits;
    next->cmp_offset   = cmp_offset;
    next->uncmp_offset = uncmp_offset;
    next->data         = point_data;

    /* */
    if (bytes_left)
        memcpy(point_data, data + index->window_size - bytes_left, bytes_left);

    /* */
    if (bytes_left < index->window_size)
        memcpy(point_data + bytes_left, data, index->window_size - bytes_left);

    zran_log("  -> [%02x %02x %02x %02x ...]\n",
             point_data[0],
             point_data[1],
             point_data[2],
             point_data[3]);
    
    index->npoints++;

    return 0;

fail:
    if (point_data != NULL) 
        free(point_data);
    
    return -1;
};


int _zran_expand_index(zran_index_t *index, uint64_t until)
{

    /* Used to store return values */
    int            ret;

    /* 
     * Counters to keep track of where we are 
     * in both the compressed and uncompressed 
     * streams.
     */
    uint64_t       cmp_offset;
    uint64_t       uncmp_offset;

    /* 
     * A reference to the second to last
     * point in the index. This is where
     *  we need to start decompressing 
     * data from before we can add more 
     * index points.
     */
    zran_point_t  *start = NULL;

    /* 
     * Uncompresse data offset of the 
     * most recent point that was added
     * to the index.
     */
    uint64_t       last_uncmp_offset;

    /* Zlib stream struct */
    z_stream       strm;

    /* Buffers to store compresed and uncompressed data */
    uint8_t       *input  = NULL;
    uint8_t       *window = NULL;

    /* 
     * This stores the base2 logarithm of the window 
     * size - it is needed to initialise zlib inflation.
     */
    int windowBits = (int)round(log10(index->window_size) / log10(2));

    /*
     * If until == 0, we build the full index, so
     * we'll set it to the compressed file size.
     */
    if (until == 0) {
        until = index->compressed_size;
    }

    /*
     * In order to create a new index 
     * oint, we need to start reading 
     * at the second to last index 
     * point, so that we read enough 
     * data to initialise the inflation.
     */
    if (index->npoints > 1) {
        
        start = &(index->list[index->npoints - 2]);

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

    zran_log("zran_expand_index(%llu)\n", until);

    /*
     * We start from the compressed data location
     * that correspods to the second to last point 
     * in the index.
     */
    if (start != NULL) {

        cmp_offset        = start->cmp_offset;
        uncmp_offset      = start->uncmp_offset;
        last_uncmp_offset = index->list[index->npoints-1].uncmp_offset;

        if (start->bits > 0)
            cmp_offset -= 1;
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

    zran_log("  Expanding from point c=%llu, u=%llu\n", cmp_offset, uncmp_offset);
    if (fseek(index->fd, cmp_offset, SEEK_SET) != 0) {
        goto fail;
    }

    /* 
     * Initialise the zlib struct for inflation
     */
    strm.zalloc   = Z_NULL;
    strm.zfree    = Z_NULL;
    strm.opaque   = Z_NULL;
    strm.avail_in = 0;
    strm.next_in  = Z_NULL;

    /* 
     * If we're starting from the beginning of the file,
     * we tell inflateInit2 to expect a file header
     */

    if (cmp_offset == 0) {
      zran_log("  Initialising for inflation from beginning\n");
        if (inflateInit2(&strm, windowBits + 32) != Z_OK) {
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
        zran_log("  Initialising for raw inflation\n");
        if (inflateInit2(&strm, -windowBits) != Z_OK) {
            goto fail;
        }

        /* The starting index point is not 
         * byte-aligned, so we'll insert 
         * the initial bits into the inflate 
         * stream using inflatePrime
         */
        if (start->bits > 0) {

            cmp_offset += 1;
            ret         = getc(index->fd);
            
            if (ret == -1) 
                goto fail;

            zran_log("  inflatePrime (%i, %i)\n",
                     start->bits,
                     ret >> (8 - start->bits));
            if (inflatePrime(&strm, start->bits, ret >> (8 - start->bits)) != Z_OK)
                goto fail;
        }

        /*
         * Initialise the inflate stream 
         * with the index point data.
         */
        zran_log("  InflateSetDictionary( %02x %02x %02x ...)\n",
                 start->data[0],
                 start->data[1],
                 start->data[2]); 
        if (inflateSetDictionary(&strm, start->data, index->window_size) != Z_OK)
            goto fail;        
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

        zran_log("  Reading from %ld\n", ftell(index->fd));

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
             */
                        zran_log("  Inflate\n");
                        zran_log("  avail_in:  %u\n", strm.avail_in);
                        zran_log("  avail_out: %u\n", strm.avail_out);
                        zran_log("  next_in:   [%02x %02x %02x %02x\n",
                                 strm.next_in[0],
                                 strm.next_in[1],
                                 strm.next_in[2],
                                 strm.next_in[3]);
            ret = inflate(&strm, Z_BLOCK);

            /*
             * Adjust our offsets according to 
             * what was actually decompressed.
             */
            cmp_offset   -= strm.avail_in;
            uncmp_offset -= strm.avail_out;

            /* 
             * Break at the end of the stream, or 
             * when we've expanded the index far 
             * enough.
             */
            if      (ret == Z_STREAM_END) break;
            else if (ret != Z_OK)         { zran_log("!= ZOK (%i)\n", ret);goto fail;}
            if      (cmp_offset >= until) break;

            /* 
             * If at end of a deflate block, consider adding an index entry
             * (note that if data_type indicates an end-of-block, then all of
             * the uncompressed data from that block has been delivered, and
             * none of the compressed data after that block has been consumed,
             * except for up to seven bits) -- the uncmp_offset == 0 provides
             * an entry point after the zlib or gzip header, and ensures that
             * the index always has at least one index point. We avoid
             * creating an index point after the last block by checking bit 6
             * of data_type.
             */
            if ( (strm.data_type & 128) &&
                !(strm.data_type & 64)  &&
                (uncmp_offset == 0 ||
                 uncmp_offset - last_uncmp_offset > index->spacing)) {

                if (_zran_add_point(index,
                                    strm.data_type & 7,
                                    cmp_offset,
                                    uncmp_offset,
                                    strm.avail_out,
                                    window) != 0) {
                    goto fail;
                }
                
                last_uncmp_offset = uncmp_offset;
            }
        } while (strm.avail_in != 0);
        
    } while (ret != Z_STREAM_END && cmp_offset < until);

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
 * the uncompressed data stream. 
 *
 * If whence is not equal to SEEK_SET, returns -1.
 */ 
int zran_seek(zran_index_t  *index,
              off_t          offset,
              int            whence,
              zran_point_t **point) {

    int           result;
    uint64_t      expand;
    zran_point_t *seek_point;

    zran_log("zran_seek(%lld, %i)\n", offset, whence);

    if (whence != SEEK_SET) {
        goto fail;
    }

    /*
     * See if there is an index point that 
     * covers the specified offset. If there;s
     * not, we're going to expand the index
     * until there is.
     *
     * TODO Maybe it is better to return a code
     *      and let the caller decide whether or 
     *      not to expand the index.
     */
    result = _zran_get_point_at(index, offset, 0, &seek_point);
    while (result > 0) {
    
        /*
         * If result > 0, get point says that an index 
         * point for this offset doesn't yet exist. So 
         * let's expand the index.
         *
         * Guess how far we need to expand the index,
         * and expand it by that much.
         */
        expand = _zran_estimate_offset(index, offset, 0);

        zran_log("Expanding index to %llu\n", expand);
        if (_zran_expand_index(index, expand) != 0)
            goto fail;

        result = _zran_get_point_at(index, offset, 0, &seek_point); 
    }

    /* 
     * get point failed - something has gone wrong 
     */
    if (result < 0) {
        goto fail;
    }

    /* get point returned success, but didn't 
     * give me a point.  Something has gone wrong.
     */ 
    if (result == 0 && seek_point == NULL) {
        goto fail;
    } 

    index->uncmp_seek_offset = offset;
    offset                   = seek_point->cmp_offset;

    if (seek_point->bits > 0)
        offset -= 1;

    zran_log("Seeking to compressed stream offset %lld\n", offset);

    if (point != NULL) {
        *point = seek_point;
    }

    return fseeko(index->fd, offset, SEEK_SET);
    
fail:
    return -1;
}


size_t zran_read(zran_index_t *index,
                 uint8_t      *buf,
                 size_t        len) {

    int           ch;
    off_t         uncmp_offset;
    off_t         cmp_offset;
    int           skip;
    z_stream      strm;
    zran_point_t *point;
    uint8_t      *input   = NULL;
    uint8_t      *discard = NULL;

    input = calloc(1, index->readbuf_size);
    if (input == NULL)
        goto fail; 
    
    discard = calloc(1, index->window_size);
    if (discard == NULL)
        goto fail;

    zran_log("zran_read(%i)\n", len);

    // silly input
    if (len == 0)
        return 0;

    // Get the current location
    // in the compressed stream.
    cmp_offset   = ftello(index->fd);
    uncmp_offset = index->uncmp_seek_offset;
             
    zran_log("Offsets: compressed=%lld, uncompressed=%lld\n",
             cmp_offset,
             uncmp_offset);

    if (cmp_offset < 0) 
        return -1;

    // Get the current index point
    // that corresponds to this
    // location.
    _zran_get_point_at(index, cmp_offset, 1, &point);

    if (point == NULL) 
        return -1;

    zran_log("Identified access point: %lld - %lld\n",
             point->cmp_offset,
             point->uncmp_offset);
    
    /* initialize file and inflate state to start there */
    strm.zalloc   = Z_NULL;
    strm.zfree    = Z_NULL;
    strm.opaque   = Z_NULL;
    strm.avail_in = 0;
    strm.next_in  = Z_NULL;
    
    if (inflateInit2(&strm, -15) != Z_OK)
        goto fail;

    // The compressed location is
    // not byte-aligned with the
    // uncompressed location.
    if (point->bits) {
        ch = getc(index->fd);
        if (ch == -1) 
            goto fail;

        zran_log("inflatePrime(%i, %i)\n",
                 point->bits,
                 ch >> (8 - point->bits)); 

        if (inflatePrime(&strm, point->bits, ch >> (8 - point->bits)) != Z_OK)
            goto fail;
    }

    zran_log("InflateSetDictionary( %02x %02x %02x ...)\n",
             point->data[0],
             point->data[1],
             point->data[2]);
    if (inflateSetDictionary(&strm, point->data, index->window_size) != Z_OK)
        goto fail;

    /* skip uncompressed bytes until offset reached, then satisfy request */
    zran_log("Initial offset: %lld - %lld\n", uncmp_offset, point->uncmp_offset);

    uncmp_offset -= point->uncmp_offset;
    
    strm.avail_in = 0;
    skip = 1; /* while skipping to offset */
    do {
        /* define where to put uncompressed data, and how much */
        if (uncmp_offset == 0 && skip) {          /* at offset now */
            strm.avail_out = len;
            strm.next_out = buf;
            skip = 0;                       /* only do this once */
        }
        if (uncmp_offset > index->window_size) {             /* skip WINSIZE bytes */
            strm.avail_out = index->window_size;
            strm.next_out = discard;
            uncmp_offset -= index->window_size;
        }
        else if (uncmp_offset != 0) {             /* last skip */
            strm.avail_out = (unsigned)uncmp_offset;
            strm.next_out = discard;
            uncmp_offset = 0;
        }

        /* uncompress until avail_out filled, or end of stream */
        do {
            if (strm.avail_in == 0) {
                strm.avail_in = fread(input, 1, index->readbuf_size, index->fd);
                if (ferror(index->fd)) {
                    goto fail;
                }
                if (strm.avail_in == 0) {
                    goto fail;
                }
                strm.next_in = input;
            }
            /* normal inflate */
            ch = inflate(&strm, Z_NO_FLUSH);

            if (ch == Z_STREAM_END)
                break;
            else if (ch != Z_OK) {
                goto fail;
            }

        } while (strm.avail_out != 0);

        /* if reach end of stream, then don't keep trying to get more */
        if (ch == Z_STREAM_END)
            break;

        /* do until offset reached and requested data read, or stream ends */
    } while (skip);

    /* compute number of uncompressed bytes read after offset */
    inflateEnd(&strm);

    len = skip ? 0 : len - strm.avail_out;

    /* Update the current seek position */
    zran_seek(index,
              index->uncmp_seek_offset + len,
              SEEK_SET,
              NULL);

    free(input);
    free(discard);
    return len;

fail:

    if (input   != NULL) free(input);
    if (discard != NULL) free(discard);
    
    inflateEnd(&strm);
    return -1; 
}
