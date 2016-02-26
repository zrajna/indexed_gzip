#include <stdio.h>
#include <Python.h>
#include <structmember.h>

#include "zran.h"


#define IDXGZIP_VERBOSE


#ifdef IDXGZIP_VERBOSE
#define igz_log(...) fprintf(stderr, __VA_ARGS__)
#else
#define igz_log(...) 
#endif


typedef struct _IndexedGzipFile {

    PyObject_HEAD
    
    PyObject    *py_fid;
    FILE        *fid;
    int          span; 
    int          size;
    int          available_size;
    zran_index_t index;
  
} IndexedGzipFile;


static PyObject * IndexedGzipFile_new(PyTypeObject *type,
                                      PyObject     *args,
                                      PyObject     *kwargs) {

    IndexedGzipFile *self;

    igz_log("IndexedGzipFile_new\n");

    self = (IndexedGzipFile *)type->tp_alloc(type, 0);

    if (self == NULL) 
        goto fail;

    zran_new(&(self->index));

    self->fid            = NULL;
    self->span           = 0;
    self->size           = 0;
    self->available_size = 0;

    return (PyObject *)self;

 fail:
    if (self != NULL) {
        Py_DECREF(self);
        return NULL;
    }

    return (PyObject *)self;
};


// ZIndex(span=1048576)
//
// todo: ZIndex(span, filename=None, fid=None, init_index=False)
//
static int IndexedGzipFile_init(IndexedGzipFile *self,
                                PyObject        *args,
                                PyObject        *kwargs) {

    PyObject *py_fid     = NULL;
    FILE     *fid        = NULL;
    int       span       = -1;
    char      init_index = -1;

    igz_log("IndexedGzipFile_init\n");

    static char *kwlist[] = {"fid", "span", "init_index", NULL};
    
    if (!PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "O|$ip",
                                     kwlist,
                                     &py_fid,
                                     &span,
                                     &init_index)) {
        goto fail;
    }

    if (span       < 0) { span       = 1048576; }
    if (init_index < 0) { init_index = 0;       }

    fid = fdopen(PyObject_AsFileDescriptor(py_fid), "rb");

    zran_init(&(self->index), span);

    self->py_fid         = py_fid;
    self->fid            = fid;
    self->span           = self->index.span;
    self->size           = self->index.size;
    self->available_size = self->index.have;

    if (init_index != 0) {
        zran_build_full_index(&(self->index), self->fid);
    }

    return 0;

fail:
    return -1;
};


static void IndexedGzipFile_dealloc(IndexedGzipFile *self) {
    
    zran_dealloc(&(self->index));

    self->fid            = NULL;
    self->span           = 0;
    self->size           = 0;
    self->available_size = 0;
};

// seek(self, offset, whence)
//
// TODO support whence != 0
//
static PyObject * IndexedGzipFile_seek(IndexedGzipFile *self, PyObject *args) {

    long          offset  = 0;
    long          whence  = 0;
    zran_point_t *point   = NULL;
    PyObject     *seekPos = NULL;

    if (!PyArg_ParseTuple(args, "|ll", &offset, &whence)) {
        goto fail;
    }

    igz_log("IndexedGzipFile_seek(%ld, %ld)\n", offset, whence);

    whence = 0;

    if (zran_seek(&(self->index),
                  self->fid,
                  offset,
                  whence,
                  &point) < 0) {
        goto fail;
    }

    seekPos = PyLong_FromLong(offset);

    return seekPos;
    
fail:
    PyErr_SetString(PyExc_RuntimeError, "wha");
    return NULL;
}


// bytes = read(len)
static PyObject * IndexedGzipFile_read(IndexedGzipFile *self, PyObject *args) {

    long           len   = 0;
    unsigned char *buf   = NULL;
    PyObject      *bytes = NULL;

    if (!PyArg_ParseTuple(args, "|l", &len)) {
        goto fail;
    }

    igz_log("IndexedGzipFile_read(%ld)\n", len);

    if (len <= 0) {
        goto fail;
    }

    buf = malloc(len);

    if (buf == NULL) {
        goto fail;
    }

    if (zran_read(&(self->index),
                  self->fid,
                  buf,
                  len) < 0) {
        goto fail;
    }

    bytes = Py_BuildValue("y#", buf, len);

    if (bytes == NULL) {
        goto fail;
    }

    free(buf);
    buf = NULL;

    return bytes;
    
fail:
    
    if (buf != NULL) {
        free(buf);
        buf = NULL;
    }
    
    PyErr_SetString(PyExc_RuntimeError, "buh");
    return NULL;
}


static struct PyMemberDef IndexedGzipFile_members[] = {

    {"fid",            T_OBJECT_EX, offsetof(IndexedGzipFile, fid),            0, "fid"},
    {"span",           T_INT,       offsetof(IndexedGzipFile, span),           0, "span"},
    {"size",           T_INT,       offsetof(IndexedGzipFile, size),           0, "size"},
    {"available_size", T_INT,       offsetof(IndexedGzipFile, available_size), 0, "available_size"},
    {NULL}
};


static struct PyMethodDef IndexedGzipFile_methods[] = {
    {"seek", (PyCFunction)IndexedGzipFile_seek, METH_VARARGS, "seek(offset, whence)"},
    {"read", (PyCFunction)IndexedGzipFile_read, METH_VARARGS, "read(len)"},
    
    {NULL}
};


static PyTypeObject IndexedGzipFile_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "indexed_gzip.IndexedGzipFile", /* tp_name */
    sizeof(IndexedGzipFile),                  /* tp_basicsize */
    0,                               /* tp_itemsize */
    (destructor)IndexedGzipFile_dealloc, /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_reserved */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash  */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    "IndexedGzipFile objects",      /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    IndexedGzipFile_methods,   /* tp_methods */
    IndexedGzipFile_members,   /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)IndexedGzipFile_init,/* tp_init */
    0,                         /* tp_alloc */
    IndexedGzipFile_new,           /* tp_new */
};


static PyModuleDef indexed_gzip_module = {
    PyModuleDef_HEAD_INIT,
    "indexed_gzip",
    "indexed_gzip",
    -1,
    NULL, NULL, NULL, NULL, NULL
};


PyMODINIT_FUNC PyInit_indexed_gzip(void) {
    PyObject *m;

    if (PyType_Ready(&IndexedGzipFile_type) < 0) {
        return NULL;
    }

    m = PyModule_Create(&indexed_gzip_module);

    if (m == NULL) {
        return NULL;
    }

    Py_INCREF(&IndexedGzipFile_type);
    PyModule_AddObject(m, "IndexedGzipFile", (PyObject *)&IndexedGzipFile_type);

    return m;
};
