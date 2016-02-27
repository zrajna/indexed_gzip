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
 * TODO
 * something like this:
 * 
 * static int _zran_invalidate_index(zran_index_t *index, off_t from);
 */


/*
 * Expands the index from its current end-point
 * until the given offset (which must be specified 
 * relative to the compressed data stream).
 *
 * If the offset is <= 0, the index is expanded to 
 * cover the entire file (i.e. it is fully built).
 *
 * Returns 0 on success, non-0 on failure.
 */
static int _zran_expand_index(
    zran_index_t *index, /* The index                         */
    FILE         *in,    /* The compressed file being indexed */
    off_t         until  /* Expand the index to this point    */
);


static int            _zran_expand_point_list(zran_index_t *index);
static int            _zran_free_unused(      zran_index_t *index);
static zran_point_t * _zran_get_point_at(     zran_index_t *index,
                                              uint64_t      offset,
                                              uint8_t       compressed);
static int            _zran_add_point(        zran_index_t *index,
                                              uint8_t       bits,
                                              off_t         cmp_offset,
                                              off_t         uncmp_offset,
                                              uint32_t      nbytes,
                                              uint8_t      *data);


int zran_init(zran_index_t *index,
              uint32_t      spacing,
              uint32_t      window_size,
              uint32_t      readbuf_size)
{

    zran_point_t *point_list = NULL;

    zran_log("zran_init(%u, %u, %u)\n", spacing, window_size, readbuf_size);

    if (spacing      == 0) spacing      = 1048576;
    if (window_size  == 0) window_size  = 32768;
    if (readbuf_size == 0) readbuf_size = 16384;

    if (window_size < 32768)
        goto fail;

    point_list = calloc(1, sizeof(zran_point_t) * 8);
    if (point_list == NULL) {
        goto fail;
    }

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


/* Deallocate an index built by build_index() */
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
    
    index->spacing           = 0;
    index->window_size       = 0;
    index->readbuf_size      = 0;
    index->npoints           = 0;
    index->size              = 0;
    index->list              = NULL;
    index->uncmp_seek_offset = 0;
};

int zran_build_index(zran_index_t *index, FILE *in)
{

    /* TODO zran_invalidate_index(index, 0); */

    return _zran_expand_index(index, in, 0);
}


zran_point_t * _zran_get_point_at(zran_index_t *index,
                                  uint64_t      offset,
                                  uint8_t       compressed) {

    zran_point_t *prev;
    zran_point_t *curr;
    uint8_t       bit;
    uint32_t      i;

    prev = index->list;

    // TODO use bsearch instead of shitty linear search
    for (i = 1; i < index->size; i++) {
        
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

    return prev;
}


/* Add an entry to the access point list. */
int _zran_add_point(zran_index_t  *index,
                    uint8_t        bits,
                    off_t          cmp_offset,
                    off_t          uncmp_offset,
                    uint32_t       nbytes,
                    uint8_t       *data) {

    zran_log("_zran_add_point(%i, %lld <-> %lld, "
             "[%02x %02x %02x %02x ... %02x %02x %02x %02x])\n",
             index->npoints,
             cmp_offset,
             uncmp_offset,
             data[0],
             data[1],
             data[2],
             data[3],
             data[nbytes - 4],
             data[nbytes - 3],
             data[nbytes - 2],
             data[nbytes - 1]);

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
    next->nbytes       = nbytes;
    next->data         = point_data;
    next->uncmp_offset = uncmp_offset;
    
    if (nbytes)
        memcpy(point_data, data + index->window_size - nbytes, nbytes);
    
    if (nbytes < index->window_size)
        memcpy(point_data + nbytes, data, index->window_size - nbytes);
    
    index->npoints++;

    return 0;

fail:
    if (point_data != NULL) 
        free(point_data);
    
    return -1;
};


int _zran_expand_index(zran_index_t *index, FILE *in, off_t until)
{

    /* Used to store return values */
    int            ret;

    /* 
     * Counters to keep track of where we are 
     * in both the compressed and uncompressed 
     * streams.
     */
    off_t          cmp_offset;
    off_t          uncmp_offset;

    /* 
     * The uncompressed offset of the last 
     * index point that was created.
     */
    off_t          last_uncmp_offset;

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
     * If until <= 0, we build the full index, so
     * we'll set it to the compressed file size.
     */
    if (until <= 0) {

        if (fseeko(in, 0, SEEK_END) != 0)
            goto fail;
        
        until = ftello(in);

        if (until < 0)
            goto fail;
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

    zran_log("zran_expand_index(%lld)\n", until);

    /* 
     * Initialise the zlib struct for inflation
     */
    strm.zalloc   = Z_NULL;
    strm.zfree    = Z_NULL;
    strm.opaque   = Z_NULL;
    strm.avail_in = 0;
    strm.next_in  = Z_NULL;

    /* Configure for automatic zlib or gzip decoding */
    if (inflateInit2(&strm, windowBits + 32) != Z_OK)
        goto fail;

    /*
     * We start from the compressed data location
     * that correspods to the most recently added
     * point in the index.
     */
    if (index->size > 0) {
        
        cmp_offset        = index->list[index->size - 1].cmp_offset;
        uncmp_offset      = index->list[index->size - 1].uncmp_offset;
        last_uncmp_offset = uncmp_offset;
    }

    /* Or the beginning of the file, if the index is empty. */
    else {
        cmp_offset        = 0;
        uncmp_offset      = 0;
        last_uncmp_offset = 0;
    }

    if (fseek(in, cmp_offset, SEEK_SET) != 0)
        goto fail;

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
        ret = fread(input, 1, index->readbuf_size, in);

        if (ret <= 0)   goto fail;
        if (ferror(in)) goto fail;

        /* 
         * Process the block of compressed 
         * data that we just read in.
         */
        strm.avail_in = ret;
        strm.next_in  = input;
        do {

            /* reset sliding window if necessary */
            if (strm.avail_out == 0) {
                strm.avail_out = index->window_size;
                strm.next_out  = window;
            }

            /* 
             * inflate until out of input, output, or at end of block --
               update the total input and output counters */
            cmp_offset   += strm.avail_in;
            uncmp_offset += strm.avail_out;

            /* 
             * Inflate the block - the decompressed 
             * data is stored in the window buffer.
             */
            ret = inflate(&strm, Z_BLOCK);

            /*
             * Adjust our offsets according to what
             * was actually decompressed.
             */
            cmp_offset   -= strm.avail_in;
            uncmp_offset -= strm.avail_out;

            /* 
             * Break at the end of the stream, or 
             * when we've expanded the index far 
             * enough.
             */
            if      (ret == Z_STREAM_END) break;
            else if (ret != Z_OK)         goto fail;
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
 * Seek to the approximate location of the specified offest into the 
 * uncompressed data stream. 
 *
 * If whence is not equal to SEEK_SET, returns -1.
 */ 
int zran_seek(zran_index_t  *index,
              FILE          *in,
              off_t          offset,
              int            whence,
              zran_point_t **point) {

    zran_point_t *seek_point;

    zran_log("zran_seek(%lld, %i)\n", offset, whence);

    if (whence != SEEK_SET) {
        return -1;
    }

    seek_point = _zran_get_point_at(index, offset, 0);

    if (seek_point == NULL) {
        return -1;
    }

    index->uncmp_seek_offset = offset;
    offset                   = seek_point->cmp_offset;

    if (seek_point->bits > 0)
        offset -= 1;

    zran_log("Seeking to compressed stream offset %lld\n", offset);

    if (point != NULL) {
        *point = seek_point;
    }

    return fseeko(in, offset, SEEK_SET);
}


size_t zran_read(zran_index_t *index,
                 FILE         *in,
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
    cmp_offset   = ftello(in);
    uncmp_offset = index->uncmp_seek_offset;
             
    zran_log("Offsets: compressed=%lld, uncompressed=%lld\n",
             cmp_offset,
             uncmp_offset);

    if (cmp_offset < 0) 
        return -1;

    // Get the current index point
    // that corresponds to this
    // location.
    point = _zran_get_point_at(index, cmp_offset, 1);

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
        ch = getc(in);
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
                strm.avail_in = fread(input, 1, index->readbuf_size, in);
                if (ferror(in)) {
                    goto fail;
                }
                if (strm.avail_in == 0) {
                    goto fail;
                }
                strm.next_in = input;
            }
            /* normal inflate */
            zran_log("Call inflate\n");
            zran_log("  Stream status:\n");
            zran_log("    avail_in:  %i\n", strm.avail_in);
            zran_log("    avail_out: %i\n", strm.avail_out);
            zran_log("    next_in:   %u\n", strm.next_in[0]);
            zran_log("    next_out:  %u\n", strm.next_out[0]);
            zran_log("    input:     %u\n", input[0]);
            zran_log("    buf:       %u\n", buf[0]);
            zran_log("    discard:   %u\n", discard[0]);
            
            ch = inflate(&strm, Z_NO_FLUSH);

            if (ch == Z_STREAM_END)
                break;
            else if (ch != Z_OK) {
                zran_log("Return code not ok: %i\n", ch);
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
              in,
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
