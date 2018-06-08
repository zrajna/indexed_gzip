# `indexed_gzip` changelog


## 0.8.3 (June 11th 2018)

* Bugfix - variious methods and attributes were missing from the
  `IndexedGzipFile` wrapper class (#9).

## 0.8.2 (March 30th 2018)

Changes in this release:

* Bugfix in deprecated `SafeIndexedGzipFile`.


## 0.8.1 (March 24th 2018)

Changes in this release:

* Updated conda metadata.


## 0.8.0 (March 24th 2018)

Changes in this release:

* Added ability to import/export the index, via new `import_index` and
  `export_index` methods on the `IndexedGzipFile` class (#7, #8) - thanks
  @ozars !
* Deprecated `SafeIndexedGzipFile` - the `IndexedGzipFile` is now
  thread-safe, and has a read buffer. If you don't want buffering
  or thread-safety, use the `_IndexedGzipFile` class.
* Fixed bug in `seek` method - was not working with negative offsets.
* More unit tests, and code coverage of `.py` and `.pyx` files.


## 0.7.1 (March 5th 2018)

Changes in this release:

* Added `.conda/meta.yaml` file for building conda packages


## 0.7.0 (November 3rd 2017)

Changes in this release:

* `IndexedGzipFile.__init__` has a new option, `drop_handles` which causes an
  `IndexedGzipFile` to close/re-open the underlying file handle on every
  access (#5, #6). This has no impact on performance (as measured by the new
  benchmark script), so is enabled by default.

* New simpler benchmark script.

* Deprecated the `fid` parameter to `IndexedGzipFile.__init__` in favour of
  `fileobj` - another change to make `IndexedGzipFile` more similar to
  `gzip.GzipFile`.


## 0.6.1 (October 4th 2017)

Changes in this release:

* `IndexedGzipFile` now has a `readinto` method.
* `SafeIndexedGzipFile` is now an `io.BufferedReader`, which provides thread
  safety and dramatically improves performance for small files (see discussion
  at nipy/nibabel#558).
* Fixed issue #4 - the source distribution was missing `zran.h`.


## 0.6.0 (September 24th 2017)

Changes in this release:

* Preliminary support for Windows platforms (#1, #3) Thanks to @mcraig-ibme
  and @Trigonometry !

* Added `mode` attribute to `IndexedGzipFile`.


## 0.5.1 (September 12th 2017)

Changes in this release:

* Unit tests are run on a 32 bit platform
* The `indexed_gzip` package now has a `__version__` attribute


## 0.5.0 (September 8th 2017)

Changes in this release:

* Re-arranged code layout - there is now a top level `indexed_gzip` package,
  which contains the `indexed_gzip` module, and the `tests` package. Done so
  that tests can be distributed properly. No API changes though.

* `zran_seek` input parameter types made more specific due to an issue on 32 bit
  platforms.


## 0.4.1 (September 6th 2017)

Changes in this release:

* A tiny adjustment to unit test management allowing tests to be run from a
  different directory


## 0.4.0 (September 5th 2017)

Changes in this release involve modifications and additions to the
`IndexedGzipFile` class to make it look and behave more like the built-in
`gzip.GzipFile` class.

* `__init__` accepts `filename` and `mode` as its first two parameters

* Changed default values for `__init__` parameters to values which have been
  qualitatively tested

* `readline`, `readlines`, `__iter__` and `__next__` methods added, for
  iteration over lines in text data

* `seek` method accepts a `whence` parameter, and allows seeking from
  `SEEK_SET` or `SEEK_CUR`,


## 0.3.3 (May 3rd 2017)

Changes in this release:

* `SafeIndexedGzipFile` was broken under python 3.


## 0.3.2 (May 1st 2017)

Changes in this release:

* Bugfix (PR #2)


## 0.3.1 (October 17th 2016)

Changes in this release:

* One critical, silly bugfix that should have been in 0.3.


## 0.3 (October 16th 2016)

Changes in this release:

* Support for reading > 2**32 bytes in a single call.
* Support for concatenated gzip streams.
* Many related bug fixes.
* `zran.c` refactored so it is much cleaner and clearer.
* Test suite is much more comprehensive


## 0.2 (August 31st 2016)

The following changes have been made in this release:

* `indexed_gzip` now releases the GIL when possible
* A new `SafeIndexedGzipFile` class provides simple thread-safe file access to
  compressed files.
* Some initial test coverage using https://travis-ci.org
* Some important bug fixes.


## 0.1 (June 16th 2016)

* First seemingly stable release.
