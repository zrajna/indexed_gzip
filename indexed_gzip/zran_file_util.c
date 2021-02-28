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
int is_readonly(FILE *fd, PyObject *f)
{
    /* Can't find a way to do this correctly under Windows and
       the check is not required anyway since the underlying
       Python module checks it already */
    return 1;
}
#else
#include <fcntl.h>
#define FSEEK fseeko
#define FTELL ftello
/* Check if file is read-only */
int is_readonly(FILE *fd, PyObject *f)
{
    return fd ? (fcntl(fileno(fd), F_GETFL) & O_ACCMODE) == O_RDONLY : 1;
}
#endif

/*
 * Implements a method analogous to fread that is performed on Python file-like objects.
 */
size_t _fread_python(void *ptr, size_t size, size_t nmemb, PyObject *f) {
    PyObject *data;
    char *buf;
    Py_ssize_t len;
    if ((data = PyObject_CallMethod(f, "read", "(n)", size * nmemb)) == NULL) {
        Py_DECREF(data);
        return 0;
    }
    if ((buf = PyBytes_AsString(data)) == NULL || (len = PyBytes_Size(data)) == -1) {
        Py_DECREF(data);
        return 0;
    }
    memmove(ptr, buf, (size_t) len);
    Py_DECREF(data);
    return (size_t) len / size;

}

/*
 * Implements a method analogous to ftell that is performed on Python file-like objects.
 */
long int _ftell_python(PyObject *f) {
    PyObject *data;
    long int result;
    if ((data = PyObject_CallMethod(f, "tell", NULL)) == NULL) {
        Py_DECREF(data);
        return -1;
    }
    if ((result = PyLong_AsLong(data)) == -1) {
        Py_DECREF(data);
        return -1;
    }
    Py_DECREF(data);
    return result;
}

/*
 * Implements a method analogous to fseek that is performed on Python file-like objects.
 */
int _fseek_python(PyObject *f, long int offset, int whence) {
    PyObject *data;
    if ((data = PyObject_CallMethod(f, "seek", "(l,i)", offset, whence)) == NULL) {
        Py_DECREF(data);
        return -1;
    }
    Py_DECREF(data);
    return 0;
}

/*
 * Implements a method analogous to feof that is performed on Python file-like objects.
 */
int _feof_python(PyObject *f, int64_t size) {
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
    if ((data = PyObject_CallMethod(f, "flush", NULL)) == NULL) {
        Py_DECREF(data);
        return -1;
    }
    Py_DECREF(data);
    return 0;
}

/*
 * Implements a method analogous to fwrite that is performed on Python file-like objects.
 */
size_t _fwrite_python(const void *ptr, size_t size, size_t nmemb, PyObject *f) {
    PyObject *input;
    PyObject *data;
    Py_ssize_t len;
    if ((input = PyBytes_FromStringAndSize(ptr, size * nmemb)) == NULL) {
        Py_DECREF(input);
        return 0;
    }
    if ((data = PyObject_CallMethod(f, "write", "(O)", input)) == NULL) {
        Py_DECREF(input);
        Py_DECREF(data);
        return 0;
    }
    if ((len = PyLong_AsSsize_t(data)) == -1) {
        Py_DECREF(input);
        Py_DECREF(data);
        return 0;
    }
    Py_DECREF(input);
    Py_DECREF(data);
    return (size_t) len / size;
}

/*
 * Implements a method analogous to getc that is performed on Python file-like objects.
 */
int _getc_python(PyObject *f) {
    char buf [1];
    if (_fread_python(buf, 1, 1, f) == 0) {
        return -1;
    }
    return (int) buf[0];
}

/*
 * Calls ferror on fd if specified, otherwise the Python-specific method on f.
 */
int ferror_(FILE *fd, PyObject *f) {
    return fd ? ferror(fd): _ferror_python(f);
}

/*
 * Calls fseek on fd if specified, otherwise the Python-specific method on f.
 */
int fseek_(FILE *fd, PyObject *f, long int offset, int whence) {
    return fd ? FSEEK(fd, offset, whence): _fseek_python(f, offset, whence);
}

/*
 * Calls ftell on fd if specified, otherwise the Python-specific method on f.
 */
int ftell_(FILE *fd, PyObject *f) {
    return fd ? FTELL(fd): _ftell_python(f);
}

/*
 * Calls fread on fd if specified, otherwise the Python-specific method on f.
 */
size_t fread_(void *ptr, size_t size, size_t nmemb, FILE *fd, PyObject *f) {
    return fd ? fread(ptr, size, nmemb, fd): _fread_python(ptr, size, nmemb, f);
}

/*
 * Calls feof on fd if specified, otherwise the Python-specific method on f.
 */
int feof_(FILE *fd, PyObject *f, int64_t size) {
    return fd ? feof(fd): _feof_python(f, size);
}

/*
 * Calls fflush on fd if specified, otherwise the Python-specific method on f.
 */
int fflush_(FILE *fd, PyObject *f) {
    return fd ? fflush(fd): _fflush_python(f);
}

/*
 * Calls fwrite on fd if specified, otherwise the Python-specific method on f.
 */
size_t fwrite_(const void *ptr, size_t size, size_t nmemb, FILE *fd, PyObject *f) {
    return fd ? fwrite(ptr, size, nmemb, fd): _fwrite_python(ptr, size, nmemb, f);
}

/*
 * Calls getc on fd if specified, otherwise the Python-specific method on f.
 */
int getc_(FILE *fd, PyObject *f) {
    return fd ? getc(fd): _getc_python(f);
}
