#ifndef __ZRAN_FILE_UTIL_H__
#define __ZRAN_FILE_UTIL_H__

/*
 * File utilities used to manipulate either
 * Python file-like objects or file descriptors.
 *
 * Author: Paul McCarthy <pauldmccarthy@gmail.com>
 */

#include <stdlib.h>
#include <stdint.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>

/*
 * These internal methods are used to apply file operations on
 * Python file-like objects that are passed to zran.
 */
size_t _fread_python(void *ptr, size_t size, size_t nmemb, PyObject *f);

int64_t _ftell_python(PyObject *f);

int _fseek_python(PyObject *f, int64_t offset, int whence);

int _feof_python(PyObject *f, int64_t size);

int _ferror_python(PyObject *f);

int _fflush_python(PyObject *f);

size_t _fwrite_python(const void *ptr, size_t size, size_t nmemb, PyObject *f);

int _getc_python(PyObject *f);

/*
 * These methods call their corresponding C methods on fd if defined, and otherwise
 * call the corresponding Python-specific method on the specified Python file-like object.
 */

int ferror_(FILE *fd, PyObject *f);

int fseek_(FILE *fd, PyObject *f, int64_t offset, int whence);

int64_t ftell_(FILE *fd, PyObject *f);

size_t fread_(void *ptr, size_t size, size_t nmemb, FILE *fd, PyObject *f);

int feof_(FILE *fd, PyObject *f, int64_t size);

int fflush_(FILE *fd, PyObject *f);

size_t fwrite_(const void *ptr, size_t size, size_t nmemb, FILE *fd, PyObject *f);

int getc_(FILE *fd, PyObject *f);

/*
 * Gets file size.
 */
int64_t fsize_(FILE *fd, PyObject *f);


#endif /* __ZRAN_H__ */
