#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "zlib.h"
#include "zran.h"


/* Demonstrate the use of build_full_index() and extract() by processing the
   file provided on the command line, and the extracting 16K from about 2/3rds
   of the way through the uncompressed output, and writing that to stdout. 
*/
int main(int argc, char **argv)
{
    int len;
    off_t offset;
    FILE *in;
    zran_index_t index;
    unsigned char buf[CHUNK];

    /* open input file */
    if (argc != 2) {
        fprintf(stderr, "usage: zran file.gz\n");
        return 1;
    }
    in = fopen(argv[1], "rb");
    if (in == NULL) {
        fprintf(stderr, "zran: could not open %s for reading\n", argv[1]);
        return 1;
    }

    /*Initialise the index*/
    zran_new( &index);
    zran_init(&index, 1048576);

    /* build index */
    len = zran_build_full_index(&index, in);

    if (len < 0) {
        fclose(in);
        switch (len) {
        case Z_MEM_ERROR:
            fprintf(stderr, "zran: out of memory\n");
            break;
        case Z_DATA_ERROR:
            fprintf(stderr, "zran: compressed data error in %s\n", argv[1]);
            break;
        case Z_ERRNO:
            fprintf(stderr, "zran: read error on %s\n", argv[1]);
            break;
        default:
            fprintf(stderr, "zran: error %d while building index\n", len);
        }
        return 1;
    }
    fprintf(stderr, "zran: built index with %d access points\n", len);

    /* use index by reading some bytes from an arbitrary offset */
    offset = (index.list[index.have - 1].uncmp_offset << 1) / 3;
    len = zran_extract(&index, in, offset, buf, CHUNK);
    if (len < 0)
        fprintf(stderr, "zran: extraction failed: %s error\n",
                len == Z_MEM_ERROR ? "out of memory" : "input corrupted");
    else {
        fwrite(buf, 1, len, stdout);
        fprintf(stderr, "zran: extracted %d bytes at %llu\n", len, offset);
    }

    /* clean up and exit */
    zran_dealloc(&index);
    fclose(in);
    return 0;
};
