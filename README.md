# indexed_gzip


 *Fast random access of gzip files in Python*


## Overview


The `indexed_gzip` project is a Python extension which aims to provide a
drop-in replacement for the built-in Python `gzip.GzipFile` class, the
`IndexedGzipFile`.


The standard `gzip.GzipFile` exposes a random access-like interface (via its
`seek` and `read` methods), but every time you seek to a new point in the
uncompressed data stream, the `GzipFile` instance decompresses from the
beginning of the file, until it reaches the requested location.


An `IndexedGzipFile` instance gets around this performance limitation by
building an index, which contains *seek points*, mappings between
corresponding locations in the compressed and uncompressed data streams. Each
seek point is accompanied by a chunk of uncompressed data which is used to
initialise the decompression algorithm, allowing us to start reading from
any seek point.


## Usage


As of March 2016, `indexed_gzip` is still under development, and is not ready
for use.  With these warning out of the way, you can use `indexed_gzip` by
following these instructions:


1. Compile the python extension:
    ```sh
    python setup.py build_ext --inplace
    ```

2. Use the `indexed_gzip` module in your Python code:
    ```python
    import indexed_gzip as igzip

    # Write support is not
    * planned at this stage
    myfile = igzip.IndexedGzipFile(open('big_file.gz', 'rb'))

    some_offset_into_uncompressed_data = 234195

    # The index will be automatically
    # built when seeking or reading .
    myfile.seek(some_offset_into_uncompressed_data)
    data = myfile.read(1048576)
    ```


A small [test script](test_indexed_gzip.py) compares the performance of the
`IndexedGzipFile` class with the `gzip.GzipFile` class.


## Acknowledgements


The `indexed_gzip` project is based upon the `zran.c` example (written by Mark
Alder) which ships with the [zlib](http://www.zlib.net/) source code.


`indexed_gzip` was originally inspired by:

    Z. Rajna, A. Keskinarkaus, V. Kiviniemi and T. Seppanen
    "Speeding up the file access of large compressed NIfTI neuroimaging data"
    Engineering in Medicine and Biology Society (EMBC), 2015 37th Annual
    International Conference of the IEEE, Milan, 2015, pp. 654-657.

    https://sourceforge.net/projects/libznzwithzindex/

Initial work on `indexed_gzip` took place at
[Brainhack](http://www.brainhack.org/) Paris, at the Institut Pasteur,
24th-26th February 2016.


## Licence

`indexed_gzip` inherits the (zlib)[http://www.zlib.net] license, available for
perusal in the [LICENSE](LICENSE) file.

