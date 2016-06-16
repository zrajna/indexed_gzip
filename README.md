# indexed_gzip


 *Fast random access of gzip files in Python*


 * [Overview](#overview)
 * [Installation](#installation)
 * [Usage](#usage)
 * [Performance](#performance)
 * [Acknowledgements](#acknowledgements)
 * [License](#license)


## Overview


The `indexed_gzip` project is a Python extension which aims to provide a
drop-in replacement for the built-in Python `gzip.GzipFile` class, the
`IndexedGzipFile`.


`indexed_gzip` was written to allow fast random access of compressed 
[NIFTI1](http://nifti.nimh.nih.gov/) image files (for which GZIP is the 
de-facto compression standard), but will work with any GZIP file. 
`indexed_gzip` is easy to use with `nibabel` 2.0.2  (http://nipy.org/nibabel/).


The standard `gzip.GzipFile` class exposes a random access-like interface (via
its `seek` and `read` methods), but every time you seek to a new point in the
uncompressed data stream, the `GzipFile` instance has to start decompressing
from the beginning of the file, until it reaches the requested location.


An `IndexedGzipFile` instance gets around this performance limitation by
building an index, which contains *seek points*, mappings between
corresponding locations in the compressed and uncompressed data streams. Each
seek point is accompanied by a chunk (32KB) of uncompressed data which is used
to initialise the decompression algorithm, allowing us to start reading from
any seek point. If the index is built with a seek point spacing of 1MB, we
only have to decompress (on average) 512KB of data to read from any location
in the file.


## Installation


1. Make sure you have [cython](http://cython.org/) installed.


2. Install into your Python environment:
   ```sh
   pip install git+https://github.com/pauldmccarthy/indexed_gzip.git
   ```


3. Or, if you don't want to install, compile the python extension:
    ```sh
    python setup.py build_ext --inplace
    ```
    
   And put the `indexed_gzip` directory on your `$PYTHONPATH`.


## Usage


You can use the `indexed_gzip` module directly:


```python
import indexed_gzip as igzip

# You can create an IndexedGzipFile instance
# by specifying a file name, or an open file
# handle. For the latter use, the file handle
# must be opened in read-only binary mode.
# Write support is currently non-existent.
myfile = igzip.IndexedGzipFile('big_file.gz')

some_offset_into_uncompressed_data = 234195

# The index will be automatically
# built on-demand when seeking or
# reading.
myfile.seek(some_offset_into_uncompressed_data)
data = myfile.read(1048576)
```


Or you can use `indexed_gzip` with `nibabel`:


```python
import nibabel      as nib
import indexed_gzip as igzip

# Here we are usin 4MB spacing between
# seek points, and using a larger read
# buffer (than the default size of 16KB).
fobj = igzip.IndexedGzipFile(
    filename='big_image.nii.gz',
    spacing=4194304,
    readbuf_size=131072)

# Create a nibabel image using 
# the existing file handle.
fmap = nib.Nifti1Image.make_file_map()
fmap['image'].fileobj = fobj
image = nib.Nifti1Image.from_file_map(fmap)
    
# Use the image ArrayProxy to access the 
# data - the index will automatically be
# built as data is accessed.
vol3 = image.dataobj[:, :, :, 3]
```

    
`indexed_gzip` does not currently have any support for writing. Currently if you 
wish to write to a file, you will need to save the file by alternate means (e.g. 
via `gzip` or `nibabel`), and then re-create a new `IndexedGzipFile` instance. 
Building on the `nibabel` example above:


```python
    
# Load the entire image into memory
data = image.get_data()
    
# Make changes to the data
data[:, :, :, 5] *= 100
    
# Save the image using nibabel
nib.save(data, 'big_image.nii.gz')
    
# Re-create an IndexedGzipFile and 
# Nifti1Image instance as above
fobj = igzip.IndexedGzipFile(...)
fmap = nib.Nifti1Image.make_file_map()
fmap['image'].fileobj = fobj
image = nib.Nifti1Image.from_file_map(fmap)
```

## Performance


A small [test script](benchmark_indexed_gzip.py) is included with `indexed_gzip`;
this script compares the performance of the `IndexedGzipFile` class with the
`gzip.GzipFile` class. This script does the following:


  1. Generates a specified number of seek locations, uniformly spaced
     throughout the input file.
  
  2. Randomly shuffles these locations

  3. Seeks to each location, and reads a chunk of data from the file.


The following table contains results of running `benchmark_indexed_gzip.py` on 
a range of different compressed files. The index was built with a seek-point 
spacing of 1048576 bytes, and was generated on-demand. Times are in seconds, 
median total time of three runs per number-of-seeks. Tests were performed on 
my laptop (MacBookPro 11,3, OSX El Capitan), while I was playing music, typing
things, and reading other things on the internet.


If you don't want to look at the numbers:

  * `GzipFile`: Total time increases linearly with respect to the number of
    file seeks.

  * `IndexedGzipFile`: Total time is pretty much constant, regardless of the
    number of file seeks.

    > This is not entirely true - total time will increase linearly with
    > respect to decompression/file I/O time, but for the file sizes/seek
    > times shown, the time taken to perform these steps is negligible.


| Compressed file size (MB) | Uncompressed file size (MB) | Number of seeks | `GzipFile` time | `IndexedGzipFile` time |
| ------------------------- | --------------------------- | --------------- | --------------- | ---------------------- |
| 1.34                      | 1.72                        | 100             | 0.47            | 0.27                   |
| 1.34                      | 1.72                        | 500             | 2.02            | 1.28                   |
| 1.34                      | 1.72                        | 1000            | 4.29            | 2.53                   |
| 1.34                      | 1.72                        | 2500            | 10.47           | 6.32                   |
|                           |                             |                 |                 |                        |
| 24.92                     | 55.09                       | 50              | 3.96            | 0.48                   |
| 24.92                     | 55.09                       | 100             | 7.98            | 0.63                   |
| 24.92                     | 55.09                       | 500             | 38.88           | 1.59                   |
| 24.92                     | 55.09                       | 1000            | 72.99           | 2.66                   |
|                           |                             |                 |                 |                        |
| 498.47                    | 1101.84                     | 1               | 2.86            | 7.43                   |
| 498.47                    | 1101.84                     | 2               | 4.92            | 7.43                   |
| 498.47                    | 1101.84                     | 3               | 6.21            | 7.26                   |
| 498.47                    | 1101.84                     | 4               | 5.88            | 7.24                   |
| 498.47                    | 1101.84                     | 6               | 11.55           | 7.28                   |
| 498.47                    | 1101.84                     | 11              | 20.91           | 6.81                   |
| 498.47                    | 1101.84                     | 21              | 35.11           | 7.30                   |
|                           |                             |                 |                 |                        |
| 1271.10                   | 2809.70                     | 1               | 7.25            | 19.07                  |
| 1271.10                   | 2809.70                     | 3               | 20.94           | 18.82                  |
| 1271.10                   | 2809.70                     | 4               | 22.07           | 18.89                  |
| 1271.10                   | 2809.70                     | 5               | 28.11           | 18.91                  |
| 1271.10                   | 2809.70                     | 6               | 35.51           | 18.97                  |
| 1271.10                   | 2809.70                     | 11              | 39.15           | 18.79                  |
|                           |                             |                 |                 |                        |
| 2542.21                   | 5619.39                     | 1               | 14.50           | 38.93                  |
| 2542.21                   | 5619.39                     | 3               | 38.51           | 35.24                  |
| 2542.21                   | 5619.39                     | 4               | 41.86           | 34.90                  |
| 2542.21                   | 5619.39                     | 5               | 48.56           | 35.01                  |
| 2542.21                   | 5619.39                     | 6               | 39.11           | 29.49                  |
| 2542.21                   | 5619.39                     | 11              | 94.50           | 37.63                  |


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
24th-26th February 2016, with the support of the 
[FMRIB Centre](https://www.ndcn.ox.ac.uk/divisions/fmrib/), at the 
University of Oxford, UK.


## License


`indexed_gzip` inherits the [zlib](http://www.zlib.net) license, available for
perusal in the [LICENSE](LICENSE) file.

