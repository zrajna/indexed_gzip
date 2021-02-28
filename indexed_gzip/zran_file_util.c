/*
 * zran_file_util.c - file utilities used in zran.c to manipulate either
 * Python file-like objects or file descriptors.
 *
 *
 * Author: Paul McCarthy <pauldmccarthy@gmail.com>
 */

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "zran_file_util.h"

#ifdef _WIN32
#define FSEEK _fseeki64
#define FTELL _ftelli64
#include "windows.h"
#include "io.h"
#else
#define FSEEK fseeko
#define FTELL ftello
#endif

/*
 * Implements a method analogous to fread that is performed on Python file-like objects.
 */
size_t _fread_python(void *ptr, size_t size, size_t nmemb, PyObject *f) {
    PyObject *data;
    char *buf;
    Py_ssize_t len;
    if ((data = PyObject_CallMethod(f, "read", "(n)", size * nmemb)) == NULL)
        goto fail;
    if ((buf = PyBytes_AsString(data)) == NULL)
        goto fail;
    if ((len = PyBytes_Size(data)) == -1)
        goto fail;
    memmove(ptr, buf, (size_t) len);
    Py_DECREF(data);
    return (size_t) len / size;

fail:
    Py_DECREF(data);
    return 0;
}

/*
 * Implements a method analogous to ftell that is performed on Python file-like objects.
 */
uint64_t _ftell_python(PyObject *f) {
    PyObject *data;
    uint64_t result;
    if ((data = PyObject_CallMethod(f, "tell", NULL)) == NULL)
        goto fail;
    // TODO: Add error handling for this statement using PyErr_Occurred
    result = PyLong_AsUnsignedLong(data);

    Py_DECREF(data);
    return result;

fail:
    Py_DECREF(data);
    return -1;
}

/*
 * Implements a method analogous to fseek that is performed on Python file-like objects.
 */
int _fseek_python(PyObject *f, long int offset, int whence) {
    PyObject *data;
    if ((data = PyObject_CallMethod(f, "seek", "(l,i)", offset, whence)) == NULL)
        goto fail;
    Py_DECREF(data);
    return 0;

fail:
    Py_DECREF(data);
    return -1;
}

/*
 * Implements a method analogous to feof that is performed on Python file-like objects.
 */
int _feof_python(PyObject *f, uint64_t size) {
    return _ftell_python(f) == size;
}

/*
 * Implements a method analogous to ferror that is performed on Python file-like objects.
 */
int _ferror_python(PyObject *f) {
    // TODO: implement better error handling for Python, using PyErr_Occurred.
    return 0;
}

/*
 * Implements a method analogous to fflush that is performed on Python file-like objects.
 */
int _fflush_python(PyObject *f) {
    PyObject *data;
    if ((data = PyObject_CallMethod(f, "flush", NULL)) == NULL) goto fail;
    Py_DECREF(data);
    return 0;

fail:
    Py_DECREF(data);
    return -1;
}

/*
 * Implements a method analogous to fwrite that is performed on Python file-like objects.
 */
size_t _fwrite_python(const void *ptr, size_t size, size_t nmemb, PyObject *f) {
    PyObject *input;
    PyObject *data = NULL;
    unsigned long len;
    if ((input = PyBytes_FromStringAndSize(ptr, size * nmemb)) == NULL)
        goto fail;
    if ((data = PyObject_CallMethod(f, "write", "(O)", input)) == NULL)
        goto fail;
    #if PY_MAJOR_VERSION >= 3
    // TODO: error handling with PyError_Occurred
    len = PyLong_AsUnsignedLong(data);
    #else
    // In Python 2, a file object's write() method does not return the number of
    // bytes written, so let's just assume that everything has been written properly.
    len = size * nmemb;
    #endif
    Py_DECREF(input);
    Py_DECREF(data);
    return (size_t) len / size;

fail:
    Py_DECREF(input);
    if (data != NULL)
        Py_DECREF(data);
    return 0;
}

/*
 * Implements a method analogous to getc that is performed on Python file-like objects.
 */
int _getc_python(PyObject *f) {
    char buf [1];
    if (_fread_python(buf, 1, 1, f) == 0) {
        // TODO: set an error indicator that can be retrieved from _ferror_python.
        return -1;
    }
    return (int) buf[0];
}

/*
 * Calls ferror on fd if specified, otherwise the Python-specific method on f.
 */
int ferror_(FILE *fd, PyObject *f) {
    return fd != NULL ? ferror(fd): _ferror_python(f);
}

/*
 * Calls fseek on fd if specified, otherwise the Python-specific method on f.
 */
int fseek_(FILE *fd, PyObject *f, long int offset, int whence) {
    return fd != NULL ? FSEEK(fd, offset, whence): _fseek_python(f, offset, whence);
}

/*
 * Calls ftell on fd if specified, otherwise the Python-specific method on f.
 */
uint64_t ftell_(FILE *fd, PyObject *f) {
    return fd != NULL ? FTELL(fd): _ftell_python(f);
}

/*
 * Calls fread on fd if specified, otherwise the Python-specific method on f.
 */
size_t fread_(void *ptr, size_t size, size_t nmemb, FILE *fd, PyObject *f) {
    return fd != NULL ? fread(ptr, size, nmemb, fd): _fread_python(ptr, size, nmemb, f);
}

/*
 * Calls feof on fd if specified, otherwise the Python-specific method on f.
 */
int feof_(FILE *fd, PyObject *f, uint64_t size) {
    return fd != NULL ? feof(fd): _feof_python(f, size);
}

/*
 * Calls fflush on fd if specified, otherwise the Python-specific method on f.
 */
int fflush_(FILE *fd, PyObject *f) {
    return fd != NULL ? fflush(fd): _fflush_python(f);
}

/*
 * Calls fwrite on fd if specified, otherwise the Python-specific method on f.
 */
size_t fwrite_(const void *ptr, size_t size, size_t nmemb, FILE *fd, PyObject *f) {
    return fd != NULL ? fwrite(ptr, size, nmemb, fd): _fwrite_python(ptr, size, nmemb, f);
}

/*
 * Calls getc on fd if specified, otherwise the Python-specific method on f.
 */
int getc_(FILE *fd, PyObject *f) {
    return fd != NULL ? getc(fd): _getc_python(f);
}
