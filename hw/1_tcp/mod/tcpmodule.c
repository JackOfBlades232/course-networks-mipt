/* 1_tcp/src/tcpmodule.c (module for python) */
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <stdio.h>
#include <assert.h>
#include <errno.h>

#define ARR_SIZE(arr_) ((sizeof(arr_)) / (sizeof((arr_)[0])))

typedef struct tcp_state {
    PyObject_HEAD
    int udp_socket;
    struct sockaddr_in remote_addr;
    uint8_t recv_buf[1 << 12];
    // uint8_t send_buf[1 << 12];
    // @TODO
} tcp_state_t;

static PyObject *tcp_error;
static char error_buf[256];

#define ERRF(fmt_, ...)                                                        \
  do {                                                                         \
    snprintf(error_buf, ARR_SIZE(error_buf), fmt_, ##__VA_ARGS__);             \
    PyErr_SetString(tcp_error, error_buf);                                     \
  } while (0)

static int tcp_init(tcp_state_t *self, PyObject *args, PyObject *)
{
    const char *local_ip_str, *remote_ip_str;
    int local_port, remote_port;

    if (!PyArg_ParseTuple(args, "(si)(si)",
                          &local_ip_str, &local_port,
                          &remote_ip_str, &remote_port))
    {
        ERRF("Arg parsing failed (__init__)");
        return -1;
    }

    struct sockaddr_in local_addr;
    local_addr.sin_family = AF_INET;
    local_addr.sin_port = htons((uint16_t)local_port);
    if (!inet_aton(local_ip_str, &local_addr.sin_addr)) {
        ERRF("Local ip is invalid");
        return -1;
    }
    
    self->remote_addr.sin_family = AF_INET;
    self->remote_addr.sin_port = htons((uint16_t)remote_port);
    if (!inet_aton(remote_ip_str, &self->remote_addr.sin_addr)) {
        ERRF("Remote ip is invalid");
        return -1;
    }

    self->udp_socket = socket(AF_INET, SOCK_DGRAM, 0);
    if (self->udp_socket == -1) {
        ERRF("Failed to open socket");
        return -1;
    }

    int opt = 1;
    setsockopt(self->udp_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    int bind_res = bind(self->udp_socket,
                        (struct sockaddr *)&local_addr,
                        sizeof(local_addr));
    if (bind_res == -1) {
        ERRF("Failed to bind socket (ip=%s(%u), port=%hu, sock=%d), error: %s",
             local_ip_str, local_addr.sin_addr.s_addr, ntohs(local_addr.sin_port),
             self->udp_socket, strerror(errno));
        close(self->udp_socket);
        return -1;
    }
    // @TODO: tcp-specific intialization

    return 0;
}

static PyObject *tcp_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    tcp_state_t *self = (tcp_state_t *)type->tp_alloc(type, 0);
    if (self != NULL) {
        int init_res = tcp_init(self, args, kwargs);
        if (init_res == -1)
            self = NULL;
    }
    return (PyObject *)self;
}

static PyObject *send_to(tcp_state_t *self, PyObject *args)
{
    Py_buffer buf;
    if (!PyArg_ParseTuple(args, "y*", &buf)) {
        ERRF("Arg parsing failed (send)");
        return NULL;
    }

    // @TODO tcp stuff

    int bytes_sent = sendto(self->udp_socket, buf.buf, buf.len, 0,
                            (const struct sockaddr *)&self->remote_addr,
                            sizeof(self->remote_addr));
    if (bytes_sent == -1) {
        ERRF("Sendto failed");
        return NULL;
    }

    PyBuffer_Release(&buf);
    return PyLong_FromLong((long)bytes_sent);
}

static PyObject *recv_from(tcp_state_t *self, PyObject *args)
{
    int n;
    if (!PyArg_ParseTuple(args, "i", &n)) {
        ERRF("Arg parsing failed (recv)");
        return NULL;
    }

    struct sockaddr_in from_addr;
    socklen_t from_addr_len;
    int bytes_received = recvfrom(self->udp_socket, &self->recv_buf,
                                  ARR_SIZE(self->recv_buf), 0,
                                  (struct sockaddr *)&from_addr,
                                  &from_addr_len);
    assert(from_addr.sin_addr == self->remote_addr.sin_addr &&
           from_addr.sin_port == self->remote_addr.sin_port);

    if (bytes_received == -1) {
        ERRF("Recvfrom failed");
        return NULL;
    }

    // @TODO tcp stuff

    return PyBytes_FromStringAndSize((const char *)self->recv_buf,
                                     (Py_ssize_t)bytes_received);
}

static PyObject *close_conn(tcp_state_t *self, PyObject *Py_UNUSED(ingnored_))
{
    // @TODO tcp stuff

    close(self->udp_socket);
    Py_RETURN_NONE;
}

static PyMethodDef tcp_methods[] =
{
    {"sendto", (PyCFunction)send_to, METH_VARARGS, "Send data over tcp."},
    {"recvfrom", (PyCFunction)recv_from, METH_VARARGS, "Receive data over tcp."},
    {"close", (PyCFunction)close_conn, METH_NOARGS, "Close tcp connection."},
    {NULL, NULL, 0, NULL} /* Sentinel */
};

static PyTypeObject tcp_state_pytype =
{
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "tcp.State",
    .tp_doc = PyDoc_STR("TCP socket state"),
    .tp_basicsize = sizeof(tcp_state_t),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = tcp_new,
    .tp_init = (initproc)tcp_init,
    .tp_methods = tcp_methods
};

static PyModuleDef tcpmodule =
{
    PyModuleDef_HEAD_INIT,
    "tcp",
    NULL,
    -1 // @TODO: what is per module state?
};

PyMODINIT_FUNC PyInit_tcp()
{
    PyObject *m;
    if (PyType_Ready(&tcp_state_pytype) < 0)
        return NULL;

    m = PyModule_Create(&tcpmodule);
    if (!m)
        return NULL;

    if (PyModule_AddObjectRef(m, "State", (PyObject *)&tcp_state_pytype) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    tcp_error = PyErr_NewException("tcp.error", NULL, NULL);
    if (PyModule_AddObjectRef(m, "error", tcp_error) < 0) {
        Py_CLEAR(tcp_error);
        Py_DECREF(m);
        return NULL;
    }

    return m;
}
