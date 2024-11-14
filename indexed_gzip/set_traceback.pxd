#
# Cython declaration for the PyException_SetTraceback function.
# This function is in the Python C API, but is not in the built-in
# Cython declarations.
#

from cpython.ref cimport PyObject

cdef extern from "Python.h":
    PyObject* PyException_SetTraceback(PyObject* ex, PyObject* tb)
