// Copyright 2016-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <Python.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <datetime.h>

#include "StrlFunc.h"
#include "xcalar/compute/localtypes/ProtoMsg.pb.h"
#include "xcalar/compute/localtypes/ParentChild.pb.h"
#include "common/Version.h"
#include "shmsg/SharedMemory.h"

#include "UdfPyXcalar.h"
#include "Utf8Verify.h"
#include "config/Config.h"
#include "constants/XcalarConfig.h"
#include "util/MemTrack.h"
#include "Child.h"
#include "xdb/Xdb.h"
#include "sys/XLog.h"
#include "strings/String.h"
#include "util/ProtoWrap.h"
#include "udf/py/UdfPyError.h"
#include "util/DFPUtils.h"
#include "SchemaProcessor.h"

const char *pyModuleName = "UdfPyXcalar";

PyMethodDef UdfPyXcalar::methods[] = {
    {"get_socket_path",
     UdfPyXcalar::getSocketPath,
     METH_NOARGS,
     "Gets UNIX domain socket path for communicating with usrnode"},

    {"get_buf_cache_base_addr",
     UdfPyXcalar::getBufCacheBaseAddr,
     METH_NOARGS,
     "returns base address of mmaped bufCache"},

    {"get_xdb_page_size",
     UdfPyXcalar::getXdbPageSize,
     METH_NOARGS,
     "Returns the size of xdbPages"},

    {"get_node_id",
     UdfPyXcalar::getNodeId,
     METH_VARARGS,
     "For a given XPU Id, return the XCE node Id\n"
     "Arguments:\n"
     "xpu_id: XPU ID"},

    {"get_node_count",
     UdfPyXcalar::getNodeCount,
     METH_NOARGS,
     "Returns node count"},

    {"get_txn_id",
     UdfPyXcalar::getTxnId,
     METH_NOARGS,
     "Returns the transaction id of the operation"},

    {"get_xpu_id", UdfPyXcalar::getXpuId, METH_NOARGS, "Returns xpu_id"},

    {"get_child_id", UdfPyXcalar::getChildId, METH_NOARGS, "Returns child_id"},

    {"get_xpu_cluster_size",
     UdfPyXcalar::getXpuClusterSize,
     METH_NOARGS,
     "Returns xpu_cluster_size"},

    {"get_config_path",
     UdfPyXcalar::getConfigPath,
     METH_NOARGS,
     "Returns path to the XCE config file"},

    {"get_user_id_name",
     UdfPyXcalar::getUserIdName,
     METH_NOARGS,
     "Returns user name string of XD user executing the app"},

    {"parse_data_page_to_csv",
     UdfPyXcalar::parseDataPageToCsv,
     METH_VARARGS,
     "Convert protobuf row to csv string"
     "Arguments:\n"
     "column_names: csv column names to pull from the datapage\n"
     "datapage: the bytestring containing a valid datapage\n"
     "field_delim: csv field delimiter\n"
     "record_delim: csv record delimiter\n"
     "quote_delim: csv quote delimiter\n"
     "format: csv or internal type"},

    {"stream_out_records",
     UdfPyXcalar::streamOutRecords,
     METH_VARARGS,
     "Streams record stream into page stream.\n\n"
     "Arguments:\n"
     "files: iterator over the file objects to load\n"
     "parse_args: argument object to the parser\n"
     "xce_dataset: instance of class XCEDataset\n"
     "apply_udf: function which takes a path and a raw_file and yields "
     "records"},

    {"convert_money_to_string",
     UdfPyXcalar::convertMoneyToString,
     METH_VARARGS,
     "Convert a 128 bit money value to String.\n\n"
     "Arguments:\n"
     "elem_0: first uint64.\n"
     "elem_1: second uint64.\n"},

    {"find_schema",
     UdfPyXcalar::findSchema,
     METH_VARARGS,
     "Find schema of JSON or Parquet data.\n\n"
     "Arguments:\n"
     "dataType: JSON|Parquet (string).\n"
     "data: JSON data or Parquet Footer data (bytes).\n"
     " - Parquet: data is last 64KiB of file or the full file if < 64KiB.\n"
     " - JSON: data is a bytes containing the JSON data.\n"},

    {"cast_columns",
     UdfPyXcalar::castColumns,
     METH_VARARGS,
     "Takes in a row dict and returns a row dict with the columns "
     " casted. Expects that all columns are present in in_row_dict.\n\n"
     "Arguments:\n"
     "in_row_dict: {'col_name': 'value_to_cast', ...}\n"
     "names_list: ['col_name', 'col_name', ...]\n"
     "types_list: ['DfString', 'DfInt64', 'DfBoolean', 'DfFloat64']\n"},

    // XXX - this takes too many generators/funcs, making it overly complicated.
    // Instead this functionality should be exposed as a python object which
    // maintains the page state, but has a function to process each
    // individual file.
    {"stream_raw_files",
     UdfPyXcalar::streamRawFiles,
     METH_VARARGS,
     "Streams given file into page stream.\n\n"
     "Arguments:\n"
     "files: iterator over the file objects to load\n"
     "parse_args: argument object to the parser\n"
     "xce_dataset: instance of class XCEDataset"},

    {"xpu_recv_string",
     UdfPyXcalar::xpuRecvString,
     METH_VARARGS,
     "Arguments:\n"
     "xpu_comm_tag: comm ctag 'ctagUser' or 'ctagBarr' on which to receive\n"
     "Returns a payload string from local XPU's recv FIFO queue"},

    {"get_xpu_start_id_in_node",
     UdfPyXcalar::getXpuStartIdInNode,
     METH_VARARGS,
     "For a given node Id, return the start Xpu Id\n"
     "Arguments:\n"
     "node_id: XCE node Id"},

    {"get_xpu_end_id_in_node",
     UdfPyXcalar::getXpuEndIdInNode,
     METH_VARARGS,
     "For a given node Id, return the end Xpu Id\n"
     "Arguments:\n"
     "node_id: XCE node Id"},

    {"xpu_send_string_fanout",
     UdfPyXcalar::xpuSendStringFanout,
     METH_VARARGS,
     "Sends supplied objects to target XPU ids in a fanout manner\n\n"
     "Arguments:\n"
     "xpu_comm_tag_str: comm ctag: 'ctagUser' or 'ctagBarr' \n"
     "page_size: size of each page in the iterator\n"
     "get_buf: function returning a buf of size page_size\n"
     "put_buf: function to append a buf \n"
     "put_last_buf: function for last buf \n"
     "send_all_bufs: final func to send list of bufs to fan out dests\n"
     "send_list: the list of (dest_xpu, payload_string) tuples "},

    {"has_running_app",
     UdfPyXcalar::isApp,
     METH_NOARGS,
     "Returns true if caller is in an App, and not a UDF"},

    {"get_app_name",
     UdfPyXcalar::getAppName,
     METH_NOARGS,
     "Returns app name if caller is in an App"},

    {"xpu_compute_num_xdb_pages_required_on_wire",
     UdfPyXcalar::xpuComputeNumXdbPagesRequiredOnWire,
     METH_VARARGS,
     "Given a message, computes and returns number of xdb pages required to "
     "send the message\n"
     "Use this to figure out number of xdb pages to allocate\n\n"
     "Arguments:\n"
     "page_size: size of each xdb page\n"
     "payload: the payload to send"},

    {"fetch_parent_perf_info",
     UdfPyXcalar::getParentPerfInfo,
     METH_NOARGS,
     "Fetches the performance info (histograms) from the parent process."},

    {NULL, NULL, 0, NULL},
};

struct PyModuleDef UdfPyXcalar::InjectedModule = {
    PyModuleDef_HEAD_INIT,
    PyModuleName,  // name
    PyHelpText,    // documentation
    -1,            // size of per-interpreter state of the module,
                   // or -1 if the module keeps state in global variables.
    methods,       // methods
    NULL,          // m_reload
    NULL,          // m_traverse
    NULL,          // m_clear
    NULL,          // m_free
};

UdfPyXcalar::UdfPyXcalar() : module(NULL) {}

UdfPyXcalar::~UdfPyXcalar() {}

PyObject *
UdfPyXcalar::initModule()
{
    PyDateTime_IMPORT;
    return PyModule_Create(&InjectedModule);
}

Status
UdfPyXcalar::injectModule()
{
    int ret = PyImport_AppendInittab(PyModuleName, initModule);
    if (ret == -1) {
        return StatusUdfPyInjectFailed;
    }
    return StatusOk;
}

PyObject *  // static
UdfPyXcalar::getSocketPath(PyObject *self, PyObject *args)
{
    char socketPath[XcalarApiMaxPathLen + 1];
    strlcpy(socketPath, Child::get()->getParentUds(), sizeof(socketPath));
    return PyUnicode_FromString(socketPath);
}

PyObject *  // static
UdfPyXcalar::getBufCacheBaseAddr(PyObject *self, PyObject *args)
{
    void *mapAddr = Child::get()->getBufCacheBaseAddr();
    if (mapAddr) {
        return PyLong_FromVoidPtr(mapAddr);
    } else {
        // try setting up shm
        Status status;
        status = Child::get()->setupShm();
        if (status == StatusOk) {
            mapAddr = Child::get()->getBufCacheBaseAddr();
            assert(mapAddr != NULL);

            return PyLong_FromVoidPtr(mapAddr);
        } else {
            return NULL;
        }
    }
}

PyObject *
UdfPyXcalar::getXdbPageSize(PyObject *self, PyObject *args)
{
    return PyLong_FromLong(XdbMgr::bcSize());
}

PyObject *
UdfPyXcalar::getChildId(PyObject *self, PyObject *args)
{
    return PyLong_FromLong(Child::get()->getChildId());
}

PyObject *
UdfPyXcalar::getNodeId(PyObject *self, PyObject *args)
{
    unsigned xpuId;

    if (!PyArg_ParseTuple(args, "i", &xpuId)) {
        xSyslog(pyModuleName,
                XlogErr,
                "UdfPyXcalar::getNodeId failed to parse argument xpuId");
        return NULL;
    }

    if (xpuId >= Child::get()->getXpuClusterSize()) {
        xSyslog(pyModuleName,
                XlogErr,
                "UdfPyXcalar::getNodeId parse argument xpuId %u is invalid",
                xpuId);
        return NULL;
    }

    unsigned ii;
    for (ii = 0; ii < Config::get()->getActiveNodes(); ii++) {
        if (xpuId >= Child::get()->getXpuStartId(ii) &&
            xpuId <= Child::get()->getXpuEndId(ii)) {
            break;
        }
    }
    assert(ii < Config::get()->getActiveNodes());

    return PyLong_FromLong(ii);
}

PyObject *
UdfPyXcalar::getNodeCount(PyObject *self, PyObject *args)
{
    return PyLong_FromLong(Config::get()->getActiveNodes());
}

PyObject *
UdfPyXcalar::getTxnId(PyObject *self, PyObject *args)
{
    return PyLong_FromLong(Txn::currentTxn().id_);
}

PyObject *
UdfPyXcalar::getXpuId(PyObject *self, PyObject *args)
{
    return PyLong_FromLong(Child::get()->getXpuId());
}

PyObject *
UdfPyXcalar::getXpuClusterSize(PyObject *self, PyObject *args)
{
    return PyLong_FromLong(Child::get()->getXpuClusterSize());
}

PyObject *
UdfPyXcalar::getConfigPath(PyObject *self, PyObject *args)
{
    return PyUnicode_FromString(Config::get()->configFilePath_);
}

PyObject *
UdfPyXcalar::getUserIdName(PyObject *self, PyObject *args)
{
    return PyUnicode_FromString(Child::get()->getUserIdName());
}

PyObject *
UdfPyXcalar::getAppName(PyObject *self, PyObject *args)
{
    return PyUnicode_FromString(Child::get()->getAppName());
}

PyObject *
UdfPyXcalar::getParentPerfInfo(PyObject *self, PyObject *args)
{
    // Python setup.
    static const char pb_name[] = "RuntimeHistogramsResponse";
    static auto database_module =
        PyImport_ImportModule("google.protobuf.symbol_database");
    static auto database =
        PyObject_CallMethod(database_module, "Default", nullptr);
    static auto msg_class =
        PyObject_CallMethod(database, "GetSymbol", "s", pb_name);

    // Convert the C++ proto object into a Python object by doing serialization.
    auto serialized_msg = Child::get()->getParentPerfInfo().SerializeAsString();
    auto py_msg = PyObject_CallObject(msg_class, nullptr);
    auto py_rv = PyObject_CallMethod(py_msg,
                                     "ParseFromString",
                                     "y#",
                                     serialized_msg.data(),
                                     serialized_msg.size());
    DCHECK(py_rv != nullptr);

    return py_msg;
}

PyObject *
UdfPyXcalar::convertMoneyToString(PyObject *self, PyObject *args)
{
    Status status;
    bool success = false;
    PyObject *ret = NULL;
    DFPUtils *dfp = DFPUtils::get();
    char buf[XLR_DFP_STRLEN + 1];
    uint64_t elem_0, elem_1;
    XlrDfp inputVal;

    if (!PyArg_ParseTuple(args, "KK", &elem_0, &elem_1)) {
        goto CommonExit;
    }
    inputVal.ieee[0] = elem_0;
    inputVal.ieee[1] = elem_1;

    dfp->xlrNumericToString(buf, &inputVal);

    ret = PyUnicode_FromString(buf);
    BailIfNull(ret);
    success = true;
CommonExit:
    if (success) {
        assert(ret);
        return ret;
    } else {
        assert(PyErr_Occurred());
        return NULL;
    }
}

// It seems that we shouldn't pass the column args in every row.
PyObject *
UdfPyXcalar::castColumns(PyObject *self, PyObject *args)
{
    Status status = StatusFailed;
    PyObject *inRowDict = NULL;
    PyObject *colNamesList = NULL;
    PyObject *colTypesList = NULL;
    PyObject *castedRow = NULL;

    PyObject *colName = NULL;
    PyObject *colType = NULL;
    PyObject *fieldVal = NULL;

    Py_ssize_t rowDictLen;
    Py_ssize_t colNamesLen;
    Py_ssize_t colTypesLen;

    // FIXME The exceptions raised here are not correct. In the case
    // of casting exceptions, we should define a custom exception
    // which we will address in the Python portion of the parser.
    // Currently, I am going to use ValueError exclusively for casting
    // exceptions and Type or RuntimeErrors for all others.
    //
    // After the above customer exception is created, we can change
    // the other exceptions to the correct Type and Value exceptions.
    if (!PyArg_ParseTuple(args,
                          "OOO",
                          &inRowDict,
                          &colNamesList,
                          &colTypesList)) {
        xSyslog(pyModuleName, XlogErr, "Failed to parser arguments.");
        goto CommonExit;
    }

    // XXX Should distinguish between row ICV exceps and caller errors.
    if (!PyDict_Check(inRowDict)) {
        PyErr_Format(PyExc_TypeError, "inRowDict is not dict");
        goto CommonExit;
    }
    if (!PyList_Check(colNamesList)) {
        PyErr_Format(PyExc_TypeError, "colNamesList is not list");
        goto CommonExit;
    }
    if (!PyList_Check(colTypesList)) {
        PyErr_Format(PyExc_TypeError, "colTypesList is not list");
        goto CommonExit;
    }

    rowDictLen = PyDict_Size(inRowDict);
    colNamesLen = PyList_Size(colNamesList);
    colTypesLen = PyList_Size(colTypesList);

    if (colNamesLen != colTypesLen) {
        PyErr_SetString(PyExc_TypeError, "colNamesLen must equal colTypesLen");
        goto CommonExit;
    }
    if (rowDictLen > colNamesLen) {
        PyErr_SetString(PyExc_TypeError,
                        "rowDictLen must be less than or equal to colNamesLen");
        goto CommonExit;
    }

    castedRow = PyDict_New();

    for (Py_ssize_t ii = 0; ii < colNamesLen; ii++) {
        char *fieldStr;  // const?
        ssize_t fieldStrSize;
        const char *colTypeStr;
        const char *colNameStr;

        // Py*_GetItem sets Index/KeyError on failure.
        // https://docs.python.org/3.3/c-api/list.html
        colType = PyList_GetItem(colTypesList, ii);
        if (!colType) {
            PyErr_Format(PyExc_TypeError,
                         "Could not get type list item index(%lu)",
                         ii);
            goto CommonExit;
        }
        Py_INCREF(colType);
        if (!PyUnicode_Check(colType)) {
            PyErr_Format(PyExc_TypeError,
                         "colTypesList[%d] is not a string",
                         ii);
            goto CommonExit;
        }

        colName = PyList_GetItem(colNamesList, ii);
        if (!colName) {
            PyErr_Format(PyExc_TypeError,
                         "Could not get column name from list(%lu)",
                         ii);
            goto CommonExit;
        }
        Py_INCREF(colName);
        if (!PyUnicode_Check(colName)) {
            PyErr_Format(PyExc_TypeError,
                         "colNamesList[%d] is not a string",
                         ii);
            goto CommonExit;
        }

        // PyUnicode_AsUTF8* sets exception on NULL return
        // It is also responsible for clearing memory
        // https://docs.python.org/3.3/c-api/unicode.html#utf-8-codecs
        colTypeStr = PyUnicode_AsUTF8(colType);
        if (!colTypeStr) {
            PyErr_Format(PyExc_TypeError,
                         "Column type must be string not (%s)",
                         colType->ob_type->tp_name);
            goto CommonExit;
        }

        colNameStr = PyUnicode_AsUTF8(colName);
        if (!colNameStr) {
            PyErr_Format(PyExc_ValueError,
                         "Column name must be string not (%s)",
                         colName->ob_type->tp_name);
            goto CommonExit;
        }

        fieldVal = PyDict_GetItem(inRowDict, colName);

        // Field is missing, make FNF (None)
        if (!fieldVal) {
            PyDict_SetItem(castedRow, colName, Py_None);
            if (colType) {
                Py_DECREF(colType);
                colType = NULL;
            }
            if (colName) {
                Py_DECREF(colName);
                colName = NULL;
            }
            continue;
        }
        Py_INCREF(fieldVal);
        if (fieldVal == Py_None) {
            // Field is present, but None
            Py_DECREF(fieldVal);
            PyDict_SetItem(castedRow, colName, Py_None);
            fieldVal = nullptr;
            if (colType) {
                Py_DECREF(colType);
                colType = NULL;
            }
            if (colName) {
                Py_DECREF(colName);
                colName = NULL;
            }
            continue;
        }

        // Should talk with PM about the messaging here.
        if (PyList_Check(fieldVal)) {
            PyErr_Format(PyExc_ValueError,
                         "Found Array in column(%s), expected(%s)",
                         colNameStr,
                         colTypeStr);
            goto CommonExit;
        } else if (PyDict_Check(fieldVal)) {
            PyErr_Format(PyExc_ValueError,
                         "Found Object in column(%s), expected(%s)",
                         colNameStr,
                         colTypeStr);
            goto CommonExit;
        } else if (PyTuple_Check(fieldVal)) {
            // This shouldn't happen as there is no Tuple in JSON
            PyErr_Format(PyExc_ValueError,
                         "Found Tuple in column(%s), expected(%s)",
                         colNameStr,
                         colTypeStr);
            goto CommonExit;
        } else if (PyBytes_Check(fieldVal)) {
            PyErr_Format(PyExc_ValueError,
                         "Found Bytes in column(%s), expected(%s)",
                         colNameStr,
                         colTypeStr);
            goto CommonExit;
        }

        // XXX As a short cut, we are just stringifying everything so that
        // I can use the existing code for casting.
        PyObject *pyFieldValStr = PyObject_Str(fieldVal);
        fieldStr = PyUnicode_AsUTF8AndSize(pyFieldValStr, &fieldStrSize);
        Py_DECREF(pyFieldValStr);

        if (!fieldStr) {
            PyErr_Format(PyExc_RuntimeError,
                         "Could not get string from "
                         "dict val with name(%s)",
                         colNameStr);
            goto CommonExit;
        }

        DfFieldType type = strToDfFieldType(colTypeStr);

        switch (type) {
        case DfBoolean: {
            if (fieldStr[0] == '\0') {
                PyDict_SetItem(castedRow, colName, Py_None);
                break;
            }
            if (fieldStrSize > 5 || fieldStrSize < 4) {
                PyErr_Format(PyExc_ValueError,
                             "%s: %s(%s) is invalid",
                             colNameStr,
                             colTypeStr,
                             fieldStr);
                goto CommonExit;
            }

            char buff[fieldStrSize + 1];  // Never more than 6
            for (int ii = 0; ii < fieldStrSize; ii++) {
                buff[ii] = tolower(fieldStr[ii]);
            }
            buff[fieldStrSize] = '\0';

            if (strncmp("true", buff, strlen("true") + 1) == 0) {
                PyDict_SetItem(castedRow, colName, Py_True);
            } else if (strncmp("false", buff, strlen("false") + 1) == 0) {
                PyDict_SetItem(castedRow, colName, Py_False);
            } else {
                PyErr_Format(PyExc_ValueError,
                             "%s: %s(%s) is invalid",
                             colNameStr,
                             colTypeStr,
                             fieldStr);
                goto CommonExit;
            }
            break;
        }
        case DfInt64: {
            if (fieldStr[0] == '\0') {
                PyDict_SetItem(castedRow, colName, Py_None);
                break;
            }

            errno = 0;
            PyObject *num = NULL;
            char *endPtr = NULL;
            char *targetEndPtr = fieldStr + fieldStrSize;
            long long int outNum = strtoll(fieldStr, &endPtr, 10);
            // We want to create a pointer to were the end of the
            // string will be so that we can check if strtoll()
            // has read all of the characters in the string.

            if (errno == EINVAL) {
                // XXX I don't have a test for this.
                PyErr_Format(PyExc_ValueError,
                             "%s: %s(%s) is invalid",
                             colNameStr,
                             colTypeStr,
                             fieldStr);
                goto CommonExit;
            }
            if (errno == ERANGE) {
                PyErr_Format(PyExc_ValueError,
                             "%s: %s(%s) exceeds range",
                             colNameStr,
                             colTypeStr,
                             fieldStr);
                goto CommonExit;
            }
            if (endPtr != targetEndPtr) {
                int offset = fieldStrSize - (targetEndPtr - endPtr);
                PyErr_Format(PyExc_ValueError,
                             "%s: %s(%s) contains non-digit "
                             "char(0x%02x) at %d",
                             colNameStr,
                             colTypeStr,
                             fieldStr,
                             fieldStr[offset],
                             offset);
                goto CommonExit;
            }

            num = PyLong_FromLongLong(outNum);
            int ret = PyDict_SetItem(castedRow, colName, num);
            Py_DECREF(num);
            if (ret != 0) {
                PyErr_Format(PyExc_RuntimeError,
                             "Could not set castedRow[%s] = %s",
                             colName,
                             fieldVal);
                goto CommonExit;
            }
            break;
        }
        case DfFloat64: {
            if (fieldStr[0] == '\0') {
                PyDict_SetItem(castedRow, colName, Py_None);
                break;
            }

            errno = 0;
            PyObject *num = NULL;
            char *endPtr = NULL;
            char *targetEndPtr = fieldStr + fieldStrSize;
            double outNum = strtod(fieldStr, &endPtr);
            // We want to create a pointer to were the end of the
            // string will be so that we can check if strtod()
            // has read all of the characters in the string.

            if (errno == ERANGE || ::isnanl(outNum) || ::isinfl(outNum)) {
                PyErr_Format(PyExc_ValueError,
                             "%s: %s(%s) exceeds range",
                             colNameStr,
                             colTypeStr,
                             fieldStr);
                goto CommonExit;
            }
            if (endPtr != targetEndPtr) {
                int offset = fieldStrSize - (targetEndPtr - endPtr);
                PyErr_Format(PyExc_ValueError,
                             "%s: %s(%s) contains non-digit "
                             "char(0x%02x) at %d",
                             colNameStr,
                             colTypeStr,
                             fieldStr,
                             fieldStr[offset],
                             offset);
                goto CommonExit;
            }
            // XXX Think about checking for precision loss

            num = PyFloat_FromDouble(outNum);
            int ret = PyDict_SetItem(castedRow, colName, num);
            Py_DECREF(num);
            if (ret != 0) {
                PyErr_Format(PyExc_RuntimeError,
                             "Could not set castedRow[%s] = '%s'",
                             colName,
                             fieldVal);
                goto CommonExit;
            }
            break;
        }
        case DfString: {
            PyObject *out = PyUnicode_FromStringAndSize(fieldStr, fieldStrSize);
            int ret = PyDict_SetItem(castedRow, colName, out);
            Py_DECREF(out);
            if (ret != 0) {
                PyErr_Format(PyExc_RuntimeError,
                             "Could not set castedRow[%s] = '%s'",
                             colName,
                             fieldVal);
                goto CommonExit;
            }
            break;
        }
        default:
            PyErr_Format(PyExc_ValueError, "Unsupported type (%s)", colTypeStr);
            goto CommonExit;
        }
        if (colType) {
            Py_DECREF(colType);
            colType = NULL;
        }
        if (colName) {
            Py_DECREF(colName);
            colName = NULL;
        }
        if (fieldVal) {
            Py_DECREF(fieldVal);
            fieldVal = NULL;
        }
    }  // End foreach field

    status = StatusOk;
CommonExit:
    if (colType) {
        Py_DECREF(colType);
        colType = NULL;
    }
    if (colName) {
        Py_DECREF(colName);
        colName = NULL;
    }
    if (fieldVal) {
        Py_DECREF(fieldVal);
        fieldVal = NULL;
    }
    if (status == StatusOk) {
        assert(castedRow);
        return castedRow;
    } else {
        if (castedRow) {
            Py_DECREF(castedRow);
            castedRow = NULL;
        }
        // All paths here should set PyExceptions
        return NULL;
    }
}

PyObject *
UdfPyXcalar::parseDataPageToCsv(PyObject *self, PyObject *args)
{
    struct DataPageColumn {
        int requestedIndex = -1;
    };
    struct BufferedCsvField {
        // We use 'lastRowSet' as a proxy for determining whether a field is
        // present in a given row or not. If we stored this as a bool, we would
        // have to reset it after every row, but if instead we store the index
        // of the last row on which this field was set, then we don't need to
        // do any bulk reseting. This is often referred to as a Generation
        // ('generation' would probably be a more appropriate name, but would
        // confuse those unfamiliar with the term).
        int64_t lastRowSet = -1;
        DataValueReader reader;
    };
    Status status;
    bool success = false;
    Py_ssize_t numRows;
    const uint8_t *dataPage;
    Py_ssize_t numCsvFields;
    Py_ssize_t dataPageLen;
    PyObject *csvFieldNameList;
    PyObject *retList = NULL;
    PyObject *stringValue = NULL;
    DataPageColumn *columns = NULL;
    BufferedCsvField *bufferedFields = NULL;
    char fieldDelim, recordDelim, quoteDelim;
    int fieldDelimInt, recordDelimInt, quoteDelimInt;
    DfFormatType formatType_ = DfFormatCsv;
    CsvBuilder csvBuilder;

    DataPageReader reader;

    // ParseTuple's 'C' type requires int, not char. This is probably because
    // of multi-byte characters that don't fit into a char or something, but
    // it's still strange.
    if (!PyArg_ParseTuple(args,
                          "Os#lCCC",
                          &csvFieldNameList,
                          &dataPage,
                          &dataPageLen,
                          &formatType_,
                          &fieldDelimInt,
                          &recordDelimInt,
                          &quoteDelimInt)) {
        goto CommonExit;
    }

    fieldDelim = fieldDelimInt;
    recordDelim = recordDelimInt;
    quoteDelim = quoteDelimInt;

    reader.init(dataPage, dataPageLen);
    assert(reader.validatePage());

    if (!PyList_Check(csvFieldNameList)) {
        PyErr_Format(PyExc_TypeError, "column_names must be a list");
        goto CommonExit;
    }

    // Parse format type
    if (formatType_ != DfFormatCsv && formatType_ != DfFormatInternal) {
        PyErr_Format(PyExc_ValueError,
                     "'format' (%i) is not a valid format type",
                     formatType_);
        goto CommonExit;
    }

    columns = new (std::nothrow) DataPageColumn[reader.getNumFields()];
    BailIfNull(columns);

    numCsvFields = PyList_Size(csvFieldNameList);
    if (PyErr_Occurred()) {
        goto CommonExit;
    }

    for (Py_ssize_t ii = 0; ii < numCsvFields; ii++) {
        PyObject *pyCsvFieldName = PyList_GetItem(csvFieldNameList, ii);
        if (!pyCsvFieldName) {
            goto CommonExit;
        }
        if (!PyUnicode_Check(pyCsvFieldName)) {
            PyErr_Format(PyExc_TypeError,
                         "csv field names must be of type str");
            goto CommonExit;
        }
        const char *csvFieldName = PyUnicode_AsUTF8(pyCsvFieldName);
        if (!csvFieldName) {
            goto CommonExit;
        }
    }
    // We now know that all of our requested CSV fields are valid.

    // XXX this could be faster. We could use a hash table to do this in
    // something like O(N+C). This is O(N*C) N=num_datapage_fields;
    // C=num_csv_fields
    // Find the appropriate csv fields for each data
    for (int ii = 0; ii < reader.getNumFields(); ii++) {
        const char *fieldName;
        status = reader.getFieldMeta(ii, &fieldName, NULL);
        BailIfFailed(status);
        for (Py_ssize_t jj = 0; jj < numCsvFields; jj++) {
            PyObject *pyCsvFieldName = PyList_GetItem(csvFieldNameList, jj);
            const char *csvFieldName = PyUnicode_AsUTF8(pyCsvFieldName);
            if (!csvFieldName) {
                goto CommonExit;
            }
            if (strcmp(csvFieldName, fieldName) == 0) {
                columns[ii].requestedIndex = jj;
                // We don't need to continue any more, since we found our field
                break;
            }
        }
    }

    numRows = reader.getNumRecords();

    retList = PyList_New(numRows);
    if (!retList) {
        goto CommonExit;
    }

    bufferedFields = new (std::nothrow) BufferedCsvField[numCsvFields];
    BailIfNull(bufferedFields);

    status = csvBuilder.init(fieldDelim, recordDelim, quoteDelim, formatType_);
    BailIfFailed(status);

    {
        DataPageReader::RecordIterator iter(&reader);
        ReaderRecord readRec;

        for (Py_ssize_t rowNum = 0; rowNum < numRows; rowNum++) {
            verify(iter.getNext(&readRec));
            ReaderRecord::FieldValueIterator fieldIter(&readRec);

            // Fetch all of the fields from this record. We are cursoring over
            // the datapage record's field sequentially, because that is the
            // fastest way to access them. The csv order might be different,
            // so we will do that part later.
            for (Py_ssize_t ii = 0; ii < reader.getNumFields(); ii++) {
                bool done;
                bool exists;

                int csvIndex = columns[ii].requestedIndex;
                BufferedCsvField *bufferedField = &bufferedFields[csvIndex];

                status = fieldIter.getNext(&done,
                                           &exists,
                                           NULL,
                                           NULL,
                                           &bufferedField->reader);
                BailIfFailed(status);

                if (exists) {
                    bufferedField->lastRowSet = rowNum;
                }

                if (done) {
                    PyErr_Format(PyExc_RuntimeError, "datapage corruption");
                    goto CommonExit;
                }
            }

            // Now we need to build our CSV row from the fields we gathered
            for (Py_ssize_t ii = 0; ii < numCsvFields; ii++) {
                BufferedCsvField *bufferedField = &bufferedFields[ii];

                bool fieldPresent = bufferedField->lastRowSet == rowNum;

                if (fieldPresent) {
                    status = csvBuilder.addField(&bufferedField->reader);
                    BailIfFailed(status);
                } else {
                    status = csvBuilder.addEmptyField();
                    BailIfFailed(status);
                }
            }

            const char *rowString;
            int64_t rowStringSize;
            status = csvBuilder.finishRow(&rowString, &rowStringSize);
            BailIfFailed(status);

            stringValue = PyUnicode_FromStringAndSize(rowString, rowStringSize);
            if (!stringValue) {
                goto CommonExit;
            }

            // This steals the reference to recordList
            int ret = PyList_SetItem(retList, rowNum, stringValue);
            if (ret != 0) {
                // XXX Does this set an exception? it's not clear
                // https://docs.python.org/3/c-api/list.html#c.PyList_SetItem
                goto CommonExit;
            }
            stringValue = NULL;
            csvBuilder.clear();
        }
    }

    success = true;
CommonExit:
    if (stringValue) {
        Py_DECREF(stringValue);
        stringValue = NULL;
    }
    if (columns) {
        delete[] columns;
        columns = NULL;
    }
    if (bufferedFields) {
        delete[] bufferedFields;
        bufferedFields = NULL;
    }
    if (status != StatusOk && !PyErr_Occurred()) {
        assert(!success && "success should be false if status is not ok");
        // If we don't have an exception, let's set one now
        PyErr_Format(PyExc_RuntimeError, status.message());
    }
    if (success) {
        assert(retList);
        return retList;
    } else {
        assert(PyErr_Occurred());
        return NULL;
    }
}

PyObject *
UdfPyXcalar::streamOutRecords(PyObject *self, PyObject *args)
{
    PyUdfProcessor *streamer = NULL;
    bool success = true;

    streamer = new (std::nothrow) PyUdfProcessor();
    if (streamer == NULL) {
        PyErr_NoMemory();
        success = false;
        goto CommonExit;
    }

    success = streamer->init(args);
    if (!success) {
        goto CommonExit;
    }
    success = streamer->run();
    if (!success) {
        goto CommonExit;
    }

CommonExit:
    if (streamer) {
        delete streamer;
        streamer = NULL;
    }
    if (success) {
        Py_RETURN_NONE;
    } else {
        assert(PyErr_Occurred());
        return NULL;
    }
}

PyObject *
UdfPyXcalar::streamRawFiles(PyObject *self, PyObject *args)
{
    PyRawStreamer *streamer = NULL;
    bool success = true;

    streamer = new (std::nothrow) PyRawStreamer();
    if (streamer == NULL) {
        PyErr_NoMemory();
        success = false;
        goto CommonExit;
    }

    success = streamer->init(args);
    if (!success) {
        goto CommonExit;
    }
    success = streamer->run();
    if (!success) {
        goto CommonExit;
    }

CommonExit:
    if (streamer) {
        delete streamer;
        streamer = NULL;
    }
    if (success) {
        Py_RETURN_NONE;
    } else {
        assert(PyErr_Occurred());
        return NULL;
    }
}

bool
PyUdfProcessor::init(PyObject *args)
{
    bool success = false;
    Status status;
    PyObject *xceDataset;
    PyObject *parseArgsObj;

    PyObject *decimalMod = PyImport_ImportModule("decimal");
    if (decimalMod == NULL) {
        goto CommonExit;
    }

    decimalClass_ = PyObject_GetAttrString(decimalMod, "Decimal");
    if (decimalClass_ == NULL) {
        goto CommonExit;
    }

    if (!PyArg_ParseTuple(args,
                          "OOOO",
                          &inFileGen_,
                          &parseArgsObj,
                          &xceDataset,
                          &parseFile_)) {
        goto CommonExit;
    }
    assert(parseFile_ != NULL && "guaranteed by ParseTuple");
    assert(xceDataset != NULL && "guaranteed by ParseTuple");

    // inFileGen arg verification
    if (!PyIter_Check(inFileGen_)) {
        PyErr_Format(PyExc_ValueError, "inFileGen is not a valid iterator");
        goto CommonExit;
    }

    // parseFile_ arg verification
    if (!PyCallable_Check(parseFile_)) {
        PyErr_Format(PyExc_ValueError, "parseFile is not a valid function");
        goto CommonExit;
    }

    success = recordStream_.init(xceDataset, parseArgsObj);
    if (!success) {
        goto CommonExit;
    }

    // XXX ENG-10363 currently we are restricting parsing to fixedSchema pages
    // to udf code path. In future, need to fix other parsers to write
    // to fixed pages.
    status =
        recordStream_.fixedSchemaWriter().init(recordStream_.xceDataset(),
                                               *recordStream_.optimizerArgs());
    if (!status.ok()) {
        PyErr_Format(PyExc_RuntimeError,
                     "failed to init fixed schema PageWriter.");
        goto CommonExit;
    }

    if (recordStream_.fixedSchemaWriter().isFixedSchema()) {
        PyObject *ret = NULL;
        // set schema as fixed, so we can process the pages as xdb pages
        ret = PyObject_CallMethod(xceDataset, "set_schema_as_fixed", NULL);
        if (ret == NULL) {
            PyErr_Format(PyExc_ValueError, "failed to set schema as fixed");
            goto CommonExit;
        }
    }

    success = true;
CommonExit:
    return success;
}

bool
PyUdfProcessor::run()
{
    Status status;
    bool success = false;

    status = recordStream_.processFiles(inFileGen_, this);
    if (status != StatusOk) {
        goto CommonExit;
    }

    success = true;

CommonExit:
    return success;
}

Status
PyUdfProcessor::processFile(const char *fileName, PyObject *file)
{
    Status status = StatusOk;

    DataPageWriter::PageStatus pageStatus;
    PyObject *recordGen = NULL;
    PyObject *recordObj = NULL;
    int32_t recordOverflow;

    PyObject *excType = NULL;
    PyObject *excValue = NULL;
    PyObject *excTraceback = NULL;
    PyObject *excStr = NULL;

    auto &fixedSchemaWriter = recordStream_.fixedSchemaWriter();

    // We need to reset recordNum_ each time we encounter a new file
    recordNum_ = 0;

    recordGen = PyObject_CallFunction(parseFile_, "sO", fileName, file);
    if (recordGen == NULL || !PyIter_Check(recordGen)) {
        PyErr_Format(PyExc_ValueError, "recordGen is not a valid iterator");
        goto CommonExit;
    }

    while ((recordObj = PyIter_Next(recordGen))) {
        bool triedOnce = false;
        if (!PyDict_Check(recordObj)) {
            status =
                recordStream_.err(recordNum_, "record is not a dictionary");
            BailIfFailed(status);
        }

        while (true) {
            if (fixedSchemaWriter.isFixedSchema()) {
                PageWriter::Record *rec = fixedSchemaWriter.newRecord();
                status = convertDictToRecord(recordObj, rec);
                BailIfFailed(status);
                status = recordStream_.addRecord(recordNum_, rec);
                if (!status.ok()) {
                    PyErr_Format(PyExc_RuntimeError, "failed to write record");
                    goto CommonExit;
                }
                break;
            } else {
                DataPageWriter::Record *writeRecord;
                status = convertDictToRecord(recordObj, &writeRecord);
                BailIfFailed(status);
                // We were unable to convert this python dictionary into a
                // record; but it was NOT an error; just continue on to the next
                // record
                if (writeRecord == NULL) {
                    break;
                }
                status = recordStream_.addRecord(recordNum_,
                                                 writeRecord,
                                                 &pageStatus,
                                                 &recordOverflow);
                if (status != StatusOk) {
                    PyErr_Format(PyExc_RuntimeError, "failed to write record");
                    status = StatusFailed;
                    goto CommonExit;
                }

                if (unlikely(pageStatus == DataPageWriter::PageStatus::Full)) {
                    if (unlikely(triedOnce)) {
                        status =
                            recordStream_
                                .err(recordNum_,
                                     "record is too large (%d bytes), "
                                     "%d over the maximum (%d)",
                                     recordStream_.writer()->pageSize() +
                                         recordOverflow,
                                     recordOverflow,
                                     recordStream_.writer()->pageCapacity());
                        BailIfFailed(status);
                        // we weren't able to add this record, but if that's
                        // nonfatal, then we should just keep going
                        break;
                    }

                    status = recordStream_.writePage();
                    BailIfFailed(status);

                    triedOnce = true;
                    continue;
                } else {
                    break;
                }
            }
        }
        ++recordNum_;
        Py_DECREF(recordObj);
        recordObj = NULL;
    }

    if (PyErr_Occurred()) {
        // PyIter_Next will return NULL whether a real error ocurred or it
        // was just the end of the generator; PyErr_Occurred distinguishes
        PyErr_Fetch(&excType, &excValue, &excTraceback);
        // Report the sUDF exception
        excStr = PyObject_Str(excValue);  // this might fail
        const char *errorStr;
        if (excStr && PyUnicode_Check(excStr) && PyUnicode_AsUTF8(excStr)) {
            errorStr = PyUnicode_AsUTF8(excStr);
        } else {
            errorStr = "streaming Scalar Function threw unknown exception";
        }
        status = recordStream_.err(recordNum_, "%s", errorStr);
        PyErr_Clear();
        BailIfFailed(status);
    }

CommonExit:
    if (recordGen != NULL) {
        Py_DECREF(recordGen);
        recordGen = NULL;
    }
    if (recordObj != NULL) {
        Py_DECREF(recordObj);
        recordObj = NULL;
    }
    if (excStr != NULL) {
        Py_DECREF(excStr);
        excStr = NULL;
    }
    if (excType != NULL) {
        Py_DECREF(excType);
        excType = NULL;
    }
    if (excValue != NULL) {
        Py_DECREF(excValue);
        excValue = NULL;
    }
    if (excTraceback != NULL) {
        Py_DECREF(excTraceback);
        excTraceback = NULL;
    }
    return status;
}

Status
PyUdfProcessor::convertDictToRecord(PyObject *dict, PageWriter::Record *record)
{
    Status status;
    PyObject *items = NULL;
    PyObject *itemIter = NULL;
    PyObject *item = NULL;

    PyObject *excType = NULL;
    PyObject *excValue = NULL;
    PyObject *excTraceback = NULL;

    // Successfully extracted a record from the dictionary
    bool success = false;

    PageWriter::FieldOfInterest *foi = NULL;
    int fieldsParsed = 0;
    auto &writer = recordStream_.fixedSchemaWriter();
    assert(writer.isFixedSchema());

    items = PyDict_Items(dict);
    itemIter = PyObject_GetIter(items);
    while ((item = PyIter_Next(itemIter))) {
        const char *key = NULL;
        PyObject *object;
        TypedDataValue dValue;

        // Pull out the field name and DataValue from the record
        bool success = PyArg_ParseTuple(item, "sO", &key, &object);
        if (success) {
            assert(key != NULL);
            foi = writer.getFieldOfInterest(key);
            if (foi == NULL) {
                Py_DECREF(item);
                item = NULL;
                continue;
            }
            success = convertObjectToDataValue(object, &dValue);
        }
        if (!success) {
            assert(PyErr_Occurred() && "NULL return implies an error");
            PyErr_Fetch(&excType, &excValue, &excTraceback);
            // Add more info to the thrown exception
            if (excValue != NULL && excType != NULL &&
                PyUnicode_Check(excValue) && PyUnicode_AsUTF8(excValue)) {
                status = recordStream_.err(recordNum_,
                                           "field '%s': %s",
                                           key ? key : "unknown",
                                           PyUnicode_AsUTF8(excValue));
                PyErr_Clear();
                BailIfFailed(status);
            } else {
                status = recordStream_.err(recordNum_,
                                           "field '%s': unknown error occurred "
                                           "converting dict",
                                           key ? key : "unknown");
                PyErr_Clear();
                BailIfFailed(status);
            }
            goto CommonExit;
        }
        status = record->setFieldByIndex(foi->idx, &dValue);
        BailIfFailed(status);
        foi = NULL;
        Py_DECREF(item);
        item = NULL;
        if (fieldsParsed++ == writer.getNumFields()) {
            break;
        }
    }

    success = true;

CommonExit:
    if (items != NULL) {
        Py_DECREF(items);
        items = NULL;
    }
    if (item != NULL) {
        Py_DECREF(item);
        item = NULL;
    }
    if (itemIter != NULL) {
        Py_DECREF(itemIter);
        itemIter = NULL;
    }
    if (excType != NULL) {
        Py_DECREF(excType);
        excType = NULL;
    }
    if (excValue != NULL) {
        Py_DECREF(excValue);
        excValue = NULL;
    }
    if (excTraceback != NULL) {
        Py_DECREF(excTraceback);
        excTraceback = NULL;
    }
    return status;
}

Status
PyUdfProcessor::convertDictToRecord(PyObject *dict,
                                    DataPageWriter::Record **record)
{
    Status status;
    PyObject *items = NULL;
    PyObject *itemIter = NULL;
    PyObject *item = NULL;

    PyObject *excType = NULL;
    PyObject *excValue = NULL;
    PyObject *excTraceback = NULL;

    DataPageWriter::Record *writeRecord;

    // Successfully extracted a record from the dictionary
    bool success = false;
    *record = NULL;

    status = recordStream_.writer()->newRecord(&writeRecord);
    BailIfFailed(status);

    assert(writeRecord && "status was StatusOk; alloc must have succeeded");

    items = PyDict_Items(dict);
    itemIter = PyObject_GetIter(items);
    while ((item = PyIter_Next(itemIter))) {
        const char *key = NULL;
        int64_t keyLen;
        PyObject *object;
        TypedDataValue dValue;

        // Pull out the field name and DataValue from the record
        bool success = PyArg_ParseTuple(item, "sO", &key, &object);
        if (success) {
            assert(key != NULL);
            keyLen = strlen(key);
            if (unlikely(key[0] == '\0')) {
                status = recordStream_.err(recordNum_,
                                           "field name is the empty string");
                BailIfFailed(status);
            }
            if (unlikely(!DataPageFieldMeta::fieldNameAllowed(key, keyLen))) {
                status = recordStream_.err(recordNum_,
                                           "field name '%s' is not allowed",
                                           key);
                BailIfFailed(status);
            }
            success = convertObjectToDataValue(object, &dValue);
        }
        if (!success) {
            assert(PyErr_Occurred() && "NULL return implies an error");
            PyErr_Fetch(&excType, &excValue, &excTraceback);
            // Add more info to the thrown exception
            if (excValue != NULL && excType != NULL &&
                PyUnicode_Check(excValue) && PyUnicode_AsUTF8(excValue)) {
                status = recordStream_.err(recordNum_,
                                           "field '%s': %s",
                                           key ? key : "unknown",
                                           PyUnicode_AsUTF8(excValue));
                PyErr_Clear();
                BailIfFailed(status);
            } else {
                status = recordStream_.err(recordNum_,
                                           "field '%s': unknown error occurred "
                                           "converting dict",
                                           key ? key : "unknown");
                PyErr_Clear();
                BailIfFailed(status);
            }

            goto CommonExit;
        }
        assert(key != NULL);
        keyLen = strlen(key);
        if (unlikely(key[0] == '\0')) {
            status =
                recordStream_.err(recordNum_, "field name is the empty string");
            BailIfFailed(status);
        }
        if (unlikely(!DataPageFieldMeta::fieldNameAllowed(key, keyLen))) {
            status = recordStream_.err(recordNum_,
                                       "field name '%s' is not allowed",
                                       key);
            BailIfFailed(status);
        }
        if (unlikely(dValue.getSize() >=
                     recordStream_.writer()->pageCapacity())) {
            // This is necessary but not sufficient; it is possible for this
            // check to indicate a valid record; but the record is still not
            // page serializable due to the field name etc.
            status = recordStream_
                         .err(recordNum_,
                              "field '%s': size(%d) larger than page size(%d)",
                              key,
                              dValue.getSize(),
                              recordStream_.writer()->pageCapacity());
            BailIfFailed(status);
        }
        status = writeRecord->addFieldByName(key, &dValue);
        if (status != StatusOk) {
            status = recordStream_.err(recordNum_,
                                       "failed to add field '%s': '%s'",
                                       key,
                                       strGetFromStatus(status));
            BailIfFailed(status);
        }
        Py_DECREF(item);
        item = NULL;
    }

    success = true;
    *record = writeRecord;

CommonExit:
    if (items != NULL) {
        Py_DECREF(items);
        items = NULL;
    }
    if (item != NULL) {
        Py_DECREF(item);
        item = NULL;
    }
    if (itemIter != NULL) {
        Py_DECREF(itemIter);
        itemIter = NULL;
    }
    if (excType != NULL) {
        Py_DECREF(excType);
        excType = NULL;
    }
    if (excValue != NULL) {
        Py_DECREF(excValue);
        excValue = NULL;
    }
    if (excTraceback != NULL) {
        Py_DECREF(excTraceback);
        excTraceback = NULL;
    }
    return status;
}

PyUdfProcessor::PyUdfProcessor() : decimalClass_(NULL)
{
    PyObject *decimalMod = PyImport_ImportModule("decimal");
    decimalClass_ = PyObject_GetAttrString(decimalMod, "Decimal");
}

PyUdfProcessor::~PyUdfProcessor()
{
    if (decimalClass_) {
        Py_DECREF(decimalClass_);
        decimalClass_ = NULL;
    }
}

bool
PyUdfProcessor::convertObjectToDataValue(PyObject *obj, TypedDataValue *value)
{
    bool success = false;
    ProtoFieldValue *pValue = NULL;
    ProtoFieldValue_ObjectValue *objectVal = NULL;
    ProtoFieldValue_ArrayValue *arrayVal = NULL;
    PyObject *strVal = NULL;
    PyObject *items = NULL;
    PyObject *itemIter = NULL;  // shared between object iter and array iter
    PyObject *item = NULL;
    PyObject *timestampObj = NULL;
    Status status = StatusOk;

    try {
        if (PyBool_Check(obj)) {
            // This must be first, because a bool is a subclass of int
            value->setBool(obj == Py_True);
        } else if (PyLong_Check(obj)) {
            PyErr_Clear();
            long val = PyLong_AsLong(obj);
            if (PyErr_Occurred()) {
                goto CommonExit;
            }
            value->setInt64(val);
        } else if (PyFloat_Check(obj)) {
            value->setFloat64(PyFloat_AsDouble(obj));
        } else if (PyUnicode_Check(obj)) {
            ssize_t strSize;
            char *str = PyUnicode_AsUTF8AndSize(obj, &strSize);
            if (str == NULL) {
                // sets an appropriate PyErr
                goto CommonExit;
            }
            value->setString(str, strSize);
        } else if (PyDateTime_Check(obj)) {
            timestampObj = PyObject_CallMethod(obj, "timestamp", NULL);
            BailIfNull(timestampObj);

            PyErr_Clear();
            double curTimeStamp = PyFloat_AsDouble(timestampObj);
            if (PyErr_Occurred()) {
                goto CommonExit;
            }

            // Cast to int64
            int64_t ms = curTimeStamp * 1000;
            value->setTimestamp(ms, 0);
        } else if (PyObject_IsInstance(obj, decimalClass_)) {
            strVal = PyObject_Str(obj);
            if (strVal) {
                XlrDfp dfp;
                ssize_t strSize;
                const char *str = PyUnicode_AsUTF8AndSize(strVal, &strSize);
                BailIfNull(str);
                assert(UTF8::isUTF8((const uint8_t *) str, strSize) &&
                       "Python should not generate invalid UTF8");

                status = DFPUtils::get()->xlrNumericFromString(&dfp, str);
                BailIfFailed(status);
                value->setNumeric(dfp);
            } else {
                // This really shouldn't happen except in allocation failure
                const char *typeName = "";
                if (obj->ob_type->tp_name != NULL) {
                    typeName = obj->ob_type->tp_name;
                }
                PyErr_Format(PyExc_ValueError,
                             "Failed to stringify field of type '%s'",
                             typeName);
                goto CommonExit;
            }
        } else if (PyDict_Check(obj)) {
            pValue = google::protobuf::Arena::CreateMessage<ProtoFieldValue>(
                recordStream_.arena());
            items = PyDict_Items(obj);
            itemIter = PyObject_GetIter(items);

            objectVal = google::protobuf::Arena::CreateMessage<
                ProtoFieldValue_ObjectValue>(recordStream_.arena());
            assert(objectVal && "will throw on alloc failure");
            while ((item = PyIter_Next(itemIter))) {
                const char *key;
                ProtoFieldValue *value =
                    google::protobuf::Arena::CreateMessage<ProtoFieldValue>(
                        recordStream_.arena());
                ConvertCtx ctx(decimalClass_, value);
                if (!PyArg_ParseTuple(item,
                                      "sO&",
                                      &key,
                                      convertObjectToProto,
                                      &ctx)) {
                    // sets an appropriate PyErr
                    goto CommonExit;
                }
                (*objectVal->mutable_values())[key] = *value;
                Py_DECREF(item);
                item = NULL;
            }
            pValue->set_allocated_objectvalue(objectVal);
            objectVal = NULL;
            value->setObject(pValue);
        } else if (PyList_Check(obj)) {
            pValue = google::protobuf::Arena::CreateMessage<ProtoFieldValue>(
                recordStream_.arena());
            itemIter = PyObject_GetIter(obj);
            arrayVal = google::protobuf::Arena::CreateMessage<
                ProtoFieldValue_ArrayValue>(recordStream_.arena());
            assert(arrayVal && "will throw on alloc failure");
            while ((item = PyIter_Next(itemIter))) {
                ProtoFieldValue *arrElm = arrayVal->add_elements();
                ConvertCtx ctx(decimalClass_, arrElm);
                bool ret = convertObjectToProto(item, &ctx);
                if (unlikely(!ret)) {
                    // failure case
                    // sets an appropriate PyErr
                    goto CommonExit;
                }
                Py_DECREF(item);
                item = NULL;
            }
            pValue->set_allocated_arrayvalue(arrayVal);
            arrayVal = NULL;
            value->setArray(pValue);
        } else if (PyBytes_Check(obj)) {
            PyErr_Format(PyExc_ValueError,
                         "Field type '%s' is not supported",
                         obj->ob_type->tp_name);
            goto CommonExit;
        } else if (obj == Py_None) {
            value->setNull();
        } else {
            // This is not a special-cased type. Try to stringify it and
            // handle strictly as a string
            strVal = PyObject_Str(obj);
            if (strVal) {
                ssize_t strSize;
                const char *str = PyUnicode_AsUTF8AndSize(strVal, &strSize);
                BailIfNull(str);
                assert(UTF8::isUTF8((const uint8_t *) str, strSize) &&
                       "Python should not generate invalid UTF8");
                value->setString(str, strSize);
            } else {
                // This really shouldn't happen except in allocation failure
                const char *typeName = "";
                if (obj->ob_type->tp_name != NULL) {
                    typeName = obj->ob_type->tp_name;
                }
                PyErr_Format(PyExc_ValueError,
                             "Failed to stringify field of type '%s'",
                             typeName);
                goto CommonExit;
            }
        }
    } catch (std::exception &e) {
        PyErr_Format(PyExc_RuntimeError,
                     "failed to set data page value: %s",
                     e.what());
        goto CommonExit;
    }

    success = true;

CommonExit:
    if (unlikely(strVal)) {
        Py_DECREF(strVal);
        strVal = NULL;
    }
    if (items != NULL) {
        Py_DECREF(items);
        items = NULL;
    }
    if (item != NULL) {
        Py_DECREF(item);
        item = NULL;
    }
    if (itemIter != NULL) {
        Py_DECREF(itemIter);
        itemIter = NULL;
    }
    if (timestampObj != NULL) {
        Py_DECREF(timestampObj);
        timestampObj = NULL;
    }
    if (recordStream_.arena() == NULL) {
        // When arena != NULL, this will be cleaned up at arena destruction
        // time
        if (unlikely(objectVal)) {
            delete objectVal;
            objectVal = NULL;
        }
        if (unlikely(arrayVal)) {
            delete arrayVal;
            arrayVal = NULL;
        }
        if (unlikely(pValue)) {
            delete pValue;
            pValue = NULL;
        }
    }
    return success;
}

bool
PyUdfProcessor::convertObjectToProto(PyObject *obj, ConvertCtx *ctx)
{
    bool success = false;
    ProtoFieldValue_ObjectValue *objectVal = NULL;
    ProtoFieldValue_ArrayValue *arrayVal = NULL;
    PyObject *strVal = NULL;
    PyObject *items = NULL;
    PyObject *itemIter = NULL;  // shared between object iter and array iter
    PyObject *item = NULL;
    PyObject *decimalModDec = ctx->decimalModDec;
    ProtoFieldValue *protoOut = ctx->value;
    google::protobuf::Arena *arena = protoOut->GetArena();
    PyObject *timestampObj = NULL;
    Status status = StatusOk;

    try {
        if (PyBool_Check(obj)) {
            // This must be first, because a bool is a subclass of int
            protoOut->set_boolval(obj == Py_True);
        } else if (PyObject_IsInstance(obj, decimalModDec)) {
            strVal = PyObject_Str(obj);
            if (strVal) {
                XlrDfp dfp;
                ssize_t strSize;
                const char *str = PyUnicode_AsUTF8AndSize(strVal, &strSize);
                BailIfNull(str);
                assert(UTF8::isUTF8((const uint8_t *) str, strSize) &&
                       "Python should not generate invalid UTF8");

                status = DFPUtils::get()->xlrNumericFromString(&dfp, str);
                BailIfFailed(status);
                for (size_t i = 0; i < ArrayLen(dfp.ieee); i++) {
                    protoOut->mutable_numericval()->add_val(dfp.ieee[i]);
                }
            } else {
                // This really shouldn't happen except in allocation failure
                const char *typeName = "";
                if (obj->ob_type->tp_name != NULL) {
                    typeName = obj->ob_type->tp_name;
                }
                PyErr_Format(PyExc_ValueError,
                             "Failed to stringify field of type '%s'",
                             typeName);
                goto CommonExit;
            }
        } else if (PyLong_Check(obj)) {
            PyErr_Clear();
            long val = PyLong_AsLong(obj);
            if (PyErr_Occurred()) {
                goto CommonExit;
            }
            protoOut->set_int64val(val);
        } else if (PyFloat_Check(obj)) {
            protoOut->set_float64val(PyFloat_AsDouble(obj));
        } else if (PyDateTime_Check(obj)) {
            google::protobuf::Timestamp *protoTimeStamp =
                google::protobuf::Arena::CreateMessage<
                    google::protobuf::Timestamp>(arena);
            assert(protoTimeStamp && "will throw on alloc failure");

            timestampObj = PyObject_CallMethod(obj, "timestamp", NULL);
            BailIfNull(timestampObj);

            double curTimeStamp;
            PyErr_Clear();
            curTimeStamp = PyFloat_AsDouble(timestampObj);
            if (PyErr_Occurred()) {
                goto CommonExit;
            }

            long sec;
            sec = long(curTimeStamp);
            long nanosec;
            nanosec = (long(curTimeStamp * NSecsPerSec) - (sec * NSecsPerSec));

            protoTimeStamp->set_seconds(sec);
            protoTimeStamp->set_nanos(nanosec);
            protoOut->set_allocated_timeval(protoTimeStamp);
        } else if (PyUnicode_Check(obj)) {
            ssize_t strSize;
            char *str = PyUnicode_AsUTF8AndSize(obj, &strSize);
            if (str == NULL) {
                // sets an appropriate PyErr
                goto CommonExit;
            }
            protoOut->set_stringval(str, strSize);
        } else if (PyDict_Check(obj)) {
            items = PyDict_Items(obj);
            itemIter = PyObject_GetIter(items);

            objectVal = google::protobuf::Arena::CreateMessage<
                ProtoFieldValue_ObjectValue>(arena);
            assert(objectVal && "will throw on alloc failure");
            while ((item = PyIter_Next(itemIter))) {
                const char *key;
                ProtoFieldValue *value =
                    google::protobuf::Arena::CreateMessage<ProtoFieldValue>(
                        arena);
                ConvertCtx ctx(decimalModDec, value);
                if (!PyArg_ParseTuple(item,
                                      "sO&",
                                      &key,
                                      convertObjectToProto,
                                      &ctx)) {
                    // sets an appropriate PyErr
                    goto CommonExit;
                }
                (*objectVal->mutable_values())[key] = *value;
                Py_DECREF(item);
                item = NULL;
            }
            protoOut->set_allocated_objectvalue(objectVal);
            objectVal = NULL;
        } else if (PyList_Check(obj)) {
            itemIter = PyObject_GetIter(obj);
            arrayVal = google::protobuf::Arena::CreateMessage<
                ProtoFieldValue_ArrayValue>(arena);
            assert(arrayVal && "will throw on alloc failure");
            while ((item = PyIter_Next(itemIter))) {
                ProtoFieldValue *arrElm = arrayVal->add_elements();
                ConvertCtx ctx(decimalModDec, arrElm);
                bool ret = convertObjectToProto(item, &ctx);
                if (unlikely(!ret)) {
                    // failure case
                    // sets an appropriate PyErr
                    goto CommonExit;
                }
                Py_DECREF(item);
                item = NULL;
            }
            protoOut->set_allocated_arrayvalue(arrayVal);
            arrayVal = NULL;
        } else if (PyBytes_Check(obj)) {
            PyErr_Format(PyExc_ValueError,
                         "Field type '%s' is not supported",
                         obj->ob_type->tp_name);
            goto CommonExit;
        } else if (obj == Py_None) {
            // Leave this as DATAVALUE_NOT_SET
        } else {
            // This is not a special-cased type. Try to stringify it and
            // handle strictly as a string
            strVal = PyObject_Str(obj);
            if (strVal) {
                ssize_t strSize;
                const char *str = PyUnicode_AsUTF8AndSize(strVal, &strSize);
                BailIfNull(str);
                assert(UTF8::isUTF8((const uint8_t *) str, strSize) &&
                       "Python should not generate invalid UTF8");
                protoOut->set_stringval(str, strSize);
            } else {
                // This really shouldn't happen except in allocation failure
                const char *typeName = "";
                if (obj->ob_type->tp_name != NULL) {
                    typeName = obj->ob_type->tp_name;
                }
                PyErr_Format(PyExc_ValueError,
                             "Failed to stringify field of type '%s'",
                             typeName);
                goto CommonExit;
            }
        }
    } catch (std::exception &e) {
        PyErr_Format(PyExc_RuntimeError,
                     "failed to set data page value: %s",
                     e.what());
        goto CommonExit;
    }

    success = true;

CommonExit:
    if (unlikely(strVal)) {
        Py_DECREF(strVal);
        strVal = NULL;
    }
    if (items != NULL) {
        Py_DECREF(items);
        items = NULL;
    }
    if (item != NULL) {
        Py_DECREF(item);
        item = NULL;
    }
    if (itemIter != NULL) {
        Py_DECREF(itemIter);
        itemIter = NULL;
    }
    if (timestampObj != NULL) {
        Py_DECREF(timestampObj);
        timestampObj = NULL;
    }
    if (arena == NULL) {
        // When arena != NULL, this will be cleaned up at arena destruction
        // time
        if (unlikely(objectVal)) {
            delete objectVal;
            objectVal = NULL;
        }
        if (unlikely(arrayVal)) {
            delete arrayVal;
            arrayVal = NULL;
        }
    }
    return success;
}

//
// PyFileReader
//

PyFileReader::~PyFileReader()
{
    if (readString_ != NULL) {
        Py_DECREF(readString_);
        readString_ = NULL;
    }
}

Status
PyFileReader::init(PyObject *inputFile)
{
    Status status = StatusOk;

    if (unlikely(inputFile == NULL)) {
        status = StatusInval;
        goto CommonExit;
    }

    inFile_ = inputFile;

CommonExit:
    return status;
}

Status
PyFileReader::readChunk(int64_t numBytes,
                        const uint8_t **data,
                        int64_t *actualBytes)
{
    Status status = StatusOk;

    // Free the string from the previous readChunk call
    if (likely(readString_ != NULL)) {
        Py_DECREF(readString_);
        readString_ = NULL;
    }

    if (numBytes != 0) {
        // Read the actual number of requested bytes
        readString_ =
            PyObject_CallMethod(inFile_, ReadFuncName, ReadArgs, numBytes);
    } else {
        // Read the whole file
        readString_ = PyObject_CallMethod(inFile_, ReadFuncName, NULL);
    }
    if (unlikely(readString_ == NULL)) {
        // An exception was thrown by 'read'
        // The exception information will be retrieved later on
        status = StatusFailed;
        goto CommonExit;
    }

    // Maybe we can let PyString_AsString convert to string force us; for
    // now mandate that the file be a str
    if (PyBytes_Check(readString_)) {
        int ret =
            PyBytes_AsStringAndSize(readString_, (char **) data, actualBytes);
        if (ret != 0) {
            assert(PyErr_Occurred());
            goto CommonExit;
        }
        assert((*data)[*actualBytes] == '\0' && "must be null terminated");
    } else if (PyUnicode_Check(readString_)) {
        *data = (uint8_t *) PyUnicode_AsUTF8AndSize(readString_, actualBytes);
    } else {
        PyErr_Format(PyExc_ValueError,
                     "file read resulted in object of type %s, not str or "
                     "bytes",
                     readString_->ob_type->tp_name);
        goto CommonExit;
    }
    // Now we can check if the above case failed
    if (*data == NULL) {
        PyErr_Format(PyExc_RuntimeError, "failed to get result of file read");
        status = StatusFailed;
        goto CommonExit;
    }

CommonExit:
    return status;
}

//
// PyRecordStreamer
//

bool
PyRecordStreamer::init(PyObject *xceDataset, PyObject *parseArgsObj)
{
    Status status;
    bool success = false;
    int64_t pageSize;

    PyObject *ret = NULL;

    assert(xceDataset);

    xceDataset_ = xceDataset;

    // parseArgs arg verification
    if (!extractParseArgs(parseArgsObj)) {
        goto CommonExit;
    }

    if (!extractParseOptimizerArgs(parseArgsObj)) {
        goto CommonExit;
    }

    ret = PyObject_CallMethod(xceDataset_, "page_size", NULL);
    if (ret == NULL) {
        assert(PyErr_Occurred() && "callMethod sets the exception");
        goto CommonExit;
    }

    if (!PyLong_Check(ret)) {
        PyErr_Format(PyExc_ValueError, "page size is not an int");
        goto CommonExit;
    }

    PyErr_Clear();  // This must be set so we can detect errors in
                    // PyLong_As...
    pageSize = PyLong_AsLongLong(ret);
    if (PyErr_Occurred()) {
        goto CommonExit;
    }

    if (pageSize <= 0) {
        PyErr_Format(PyExc_ValueError,
                     "pageSize(%lu) must be greater than 0",
                     pageSize);
        goto CommonExit;
    }

    status = writer_.init(pageSize);
    if (status != StatusOk) {
        PyErr_Format(PyExc_RuntimeError,
                     "failed to init page writer '%s'",
                     strGetFromStatus(status));
        goto CommonExit;
    }

    status = errorWriter_.init(pageSize);
    if (status != StatusOk) {
        PyErr_Format(PyExc_RuntimeError,
                     "failed to init error page writer '%s'",
                     strGetFromStatus(status));
        goto CommonExit;
    }

    success = true;

CommonExit:
    if (ret != NULL) {
        Py_DECREF(ret);
        ret = NULL;
    }
    return success;
}

Status
PyRecordStreamer::processFiles(PyObject *fileGen, IFileParser *parser)
{
    Status status = StatusOk;
    PyObject *fileTuple = NULL;
    PyObject *ret = NULL;
    unsigned long hours, minutesLeftOver, secondsLeftOver, millisecondsLeftOver;
    Stopwatch stopwatch;

    while ((fileTuple = PyIter_Next(fileGen))) {
        const char *fileName;
        int64_t fileSize;
        int64_t uncompressedFileSize;
        PyObject *fileObj;
        PyObject *sourceArgsObj;
        if (!PyArg_ParseTuple(fileTuple,
                              "OLLsO",
                              &fileObj,
                              &fileSize,
                              &uncompressedFileSize,
                              &fileName,
                              &sourceArgsObj)) {
            status = StatusFailed;
            goto CommonExit;
        }

        assert(fileReport_.fullPath == NULL);
        fileReport_.fullPath = strAllocAndCopy(fileName);
        BailIfNull(fileReport_.fullPath);
        fileReport_.fullPathLen = strlen(fileReport_.fullPath);

        bool success = extractSourceArgs(sourceArgsObj);
        if (!success) {
            if (PyErr_Occurred()) {
                pyExceptionToReport();
            }
            status = StatusInval;
            goto CommonExit;
        }

        status = parser->processFile(fileName, fileObj);
        if (status == StatusRecordError && parseArgs_.allowFileErrors) {
            status = StatusOk;
        }
        // XXX this might not be exactly correct; how do we treat the
        // interaction between python exceptions and return statuses?
        // The current thinking is to handle them distinctly; the parser may
        // choose to ignore the python exception for a reason that it knows
        // is valid. If the parser knows that it should fail, it should be
        // returning a non-zero status anyway.
        // Let's thus let pyExceptionToReport decide whether we fail from
        // the python exception or not. The common error case is probably a
        // bad return status AND a py exc.
        if (PyErr_Occurred()) {
            Status status2 = pyExceptionToReport();
            assert(!PyErr_Occurred());
            if (status == StatusOk && status2 != StatusOk) {
                status = status2;
            }
        }
        if (status.ok()) {
            status = reportFile();
        }

        stopwatch.stop();
        stopwatch.getPrintableTime(hours,
                                   minutesLeftOver,
                                   secondsLeftOver,
                                   millisecondsLeftOver);
        xSyslog(pyModuleName,
                status.ok() ? XlogInfo : XlogErr,
                "File load complete using XPU ID: %lu, "
                "target: '%s', full path: '%s', "
                "file size: %lu, uncompressed file size: %lu, "
                "num records: %lu, data size: %lu, "
                "and finished in %lu:%02lu:%02lu.%03lu: %s",
                Child::get()->getXpuId(),
                fileReport_.sourceArgs.targetName,
                fileReport_.fullPath,
                fileSize,
                uncompressedFileSize,
                fileReport_.numRecordsInFile,
                fileReport_.fileDataSize,
                hours,
                minutesLeftOver,
                secondsLeftOver,
                millisecondsLeftOver,
                strGetFromStatus(status));
        BailIfFailed(status);

        Py_DECREF(fileTuple);
        fileTuple = NULL;
        fileSize = -1;
        fileName = NULL;
        stopwatch.restart();
        fileReport_.reset();
    }
    if (PyErr_Occurred()) {
        // PyIter_Next will return NULL whether a real error ocurred or it
        // was just the end of the generator; PyErr_Occurred distinguishes
        pyExceptionToReport();
        // We don't allow failing during the iteration of files itself
        status = StatusFailed;
        goto CommonExit;
    }

    status = dumpPage(false);
    if (status != StatusOk) {
        if (!PyErr_Occurred()) {
            PyErr_Format(PyExc_RuntimeError, "failed flush last datapage");
        }
        pyExceptionToReport();
        goto CommonExit;
    }

    status = dumpPage(true);
    if (status != StatusOk) {
        if (!PyErr_Occurred()) {
            PyErr_Format(PyExc_RuntimeError, "failed flush last error page");
        }
        pyExceptionToReport();
        goto CommonExit;
    }

CommonExit:
    assert(!PyErr_Occurred());
    if (fileTuple != NULL) {
        Py_DECREF(fileTuple);
        fileTuple = NULL;
    }
    if (ret != NULL) {
        Py_DECREF(ret);
        ret = NULL;
    }
    // Now we want to propogate the last error as an exception
    if (status != StatusOk) {
        if (fileReport_.errors.size() > 0) {
            raiseExceptionFromReport();
        } else {
            PyErr_Format(PyExc_RuntimeError,
                         "processing failed: %s",
                         strGetFromStatus(status));
        }
    }

    return status;
}

Status
PyRecordStreamer::writePage()
{
    return dumpPage(false);
}

Status
PyRecordStreamer::dumpPage(bool isError)
{
    Status status = StatusOk;
    PyObject *pageObject = NULL;
    uint8_t *page;
    PyObject *ret = NULL;
    DataPageWriter *pageWriter;
    google::protobuf::Arena *pageArena;

    if (isError) {
        pageWriter = &errorWriter_;
        pageArena = &errorArena_;
    } else {
        pageWriter = &writer_;
        pageArena = &arena_;
    }

    // In fixed schema mode, we only use fixed schema writer
    // which writes as XDB page format instead of as Data page format.
    if (fixedSchemaPageWriter_.isFixedSchema()) {
        pageArena->Reset();
        status = fixedSchemaPageWriter_.shipPage();
        return status;
    }

    // Get a new shared memory page
    pageObject = PyObject_CallMethod(xceDataset_, "get_buffer", NULL);
    page = getPageFromObject(pageObject);
    if (page == NULL) {
        // PyErr must have already been set
        status = StatusFailed;
        goto CommonExit;
    }

    Py_DECREF(pageObject);
    pageObject = NULL;

    pageWriter->serializeToPage(page);
    pageWriter->clear();
    // We might want to reset this at a different cadence, such as by
    // checking its space used and having a maximum; this results in greater
    // transient memory usage, but slightly higher performance
    pageArena->Reset();

    // Inform XCE that this page is ready to be processed
    ret = PyObject_CallMethod(xceDataset_,
                              "finish_buffer",
                              "lO",
                              page,
                              isError ? Py_True : Py_False);
    if (ret == NULL) {
        status = StatusFailed;
        goto CommonExit;
    }

CommonExit:
    if (ret != NULL) {
        Py_DECREF(ret);
        ret = NULL;
    }
    if (pageObject != NULL) {
        Py_DECREF(pageObject);
        pageObject = NULL;
    }

    if (isError) {
        // We just flushed them out so we don't need to keep track anymore
        numErrors_ = 0;
    }

    return status;
}

uint8_t *
PyRecordStreamer::getPageFromObject(PyObject *pageObj)
{
    if (pageObj == NULL) {
        PyErr_Format(PyExc_RuntimeError,
                     "Out of resources (Xcalar Managed Memory)");
        return NULL;
    }
    if (!PyLong_Check(pageObj)) {
        PyErr_Format(PyExc_ValueError,
                     "dataset page generator did not provide valid page");
        return NULL;
    }

    PyErr_Clear();  // This must be set so we can detect errors in
                    // PyLong_As...
    long pageVal = PyLong_AsLongLong(pageObj);
    if (PyErr_Occurred()) {
        return NULL;
    }
    return reinterpret_cast<uint8_t *>(pageVal);
}

bool
PyRecordStreamer::extractStringFromDict(PyObject *dict,
                                        const char *dictName,
                                        const char *keyName,
                                        char *result,
                                        int maxResultSize)
{
    bool success = false;
    const char *fieldValue;

    PyObject *strValue = PyDict_GetItemString(dict, keyName);
    if (!strValue) {
        PyErr_Format(PyExc_KeyError, keyName);
        goto CommonExit;
    }

    if (!PyUnicode_Check(strValue)) {
        PyErr_Format(PyExc_ValueError,
                     "%s['%s'] is not a str",
                     dictName,
                     keyName);
        goto CommonExit;
    }
    fieldValue = PyUnicode_AsUTF8(strValue);
    if (!fieldValue) {
        goto CommonExit;
    }
    if ((int) strlcpy(result, fieldValue, maxResultSize) >= maxResultSize) {
        PyErr_Format(PyExc_ValueError, "%s['%s'] is too long");
        goto CommonExit;
    }

    success = true;

CommonExit:
    return success;
}

bool
PyRecordStreamer::extractSourceArgs(PyObject *sourceArgsObj)
{
    bool success = false;
    bool valueSuccess;
    DataSourceArgs *sourceArgs = &fileReport_.sourceArgs;
    const char *dictName = "sourceArgs";

    if (!PyDict_Check(sourceArgsObj)) {
        PyErr_Format(PyExc_ValueError, "sourceArgs is not a dictionary");
        goto CommonExit;
    }

    valueSuccess = extractStringFromDict(sourceArgsObj,
                                         dictName,
                                         "targetName",
                                         sourceArgs->targetName,
                                         sizeof(sourceArgs->targetName));
    if (!valueSuccess) {
        goto CommonExit;
    }

    valueSuccess = extractStringFromDict(sourceArgsObj,
                                         dictName,
                                         "path",
                                         sourceArgs->path,
                                         sizeof(sourceArgs->path));
    if (!valueSuccess) {
        goto CommonExit;
    }

    valueSuccess = extractStringFromDict(sourceArgsObj,
                                         dictName,
                                         "fileNamePattern",
                                         sourceArgs->fileNamePattern,
                                         sizeof(sourceArgs->fileNamePattern));
    if (!valueSuccess) {
        goto CommonExit;
    }

    // Parse recursive
    {
        PyObject *recursive = PyDict_GetItemString(sourceArgsObj, "recursive");
        if (!recursive) {
            PyErr_Format(PyExc_KeyError, "recursive");
            goto CommonExit;
        }

        if (!PyBool_Check(recursive)) {
            PyErr_Format(PyExc_ValueError,
                         "%s['recursive'] is not a bool",
                         dictName);
            goto CommonExit;
        }

        sourceArgs->recursive = recursive == Py_True;
    }

    success = true;

CommonExit:
    return success;
}

// Only parses the subset of fields which are relevant to this stage of
// parsing
bool
PyRecordStreamer::extractParseOptimizerArgs(PyObject *parseArgsObj)
{
    bool success = false;
    memZero(&optimizerArgs_, sizeof(optimizerArgs_));

    if (!PyDict_Check(parseArgsObj)) {
        PyErr_Format(PyExc_ValueError, "parseArgs is not a dictionary");
        goto CommonExit;
    }

    // Parse fields required
    {
        PyObject *fields = PyDict_GetItemString(parseArgsObj, "fieldsRequired");
        if (!fields) {
            PyErr_Format(PyExc_KeyError, "fieldsRequired");
            goto CommonExit;
        }

        if (!PyList_Check(fields)) {
            PyErr_Format(PyExc_ValueError,
                         "parseArgs['fieldsRequired'] is not a list");
            goto CommonExit;
        }

        optimizerArgs_.numFieldsRequired = PyList_Size(fields);
        for (int ii = 0; ii < PyList_Size(fields); ii++) {
            PyObject *field = PyList_GetItem(fields, ii);
            if (!field) {
                PyErr_Format(PyExc_IndexError, "fields index out of range");
                goto CommonExit;
            }
            if (!PyDict_Check(field)) {
                PyErr_Format(PyExc_ValueError,
                             "parseArgs['fieldsRequired'][%i] is not a "
                             "dictionary",
                             ii);
                goto CommonExit;
            }

            // field names we receive from usrnode are prefixed.
            PyObject *fieldName = PyDict_GetItemString(field, "fieldName");
            if (!fieldName) {
                goto CommonExit;
            }
            const char *fieldNameStr = PyUnicode_AsUTF8(fieldName);
            if (!fieldNameStr) {
                goto CommonExit;
            }

            const char *prefixPtr, *fieldNameNoPrefix;
            prefixPtr = strstr(fieldNameStr, DfFatptrPrefixDelimiterReplaced);
            if (prefixPtr != NULL) {
                fieldNameNoPrefix =
                    prefixPtr + strlen(DfFatptrPrefixDelimiterReplaced);
            } else {
                fieldNameNoPrefix = fieldNameStr;
            }

            strlcpy(optimizerArgs_.fieldNames[ii],
                    fieldNameNoPrefix,
                    sizeof(optimizerArgs_.fieldNames[0]));
            strlcpy(optimizerArgs_.fieldNamesPrefixed[ii],
                    fieldNameStr,
                    sizeof(optimizerArgs_.fieldNamesPrefixed[0]));

            PyObject *type = PyDict_GetItemString(field, "fieldType");
            if (!type) {
                goto CommonExit;
            }
            const char *typeStr = PyUnicode_AsUTF8(type);
            if (!typeStr) {
                goto CommonExit;
            }
            optimizerArgs_.valueTypes[ii] = strToDfFieldType(typeStr);
        }
        PyObject *evalStrPObj =
            PyDict_GetItemString(parseArgsObj, "evalString");
        if (!evalStrPObj) {
            goto CommonExit;
        }
        const char *evalStr = PyUnicode_AsUTF8(evalStrPObj);
        if (!evalStr) {
            goto CommonExit;
        }
        strlcpy(optimizerArgs_.evalString,
                evalStr,
                sizeof(optimizerArgs_.evalString));
    }

    success = true;
CommonExit:
    return success;
}

// Only parses the subset of fields which are relevant to this stage of
// parsing
bool
PyRecordStreamer::extractParseArgs(PyObject *parseArgsObj)
{
    bool success = false;
    bool valueSuccess;
    int fieldNameLen;
    memZero(&parseArgs_, sizeof(parseArgs_));

    if (!PyDict_Check(parseArgsObj)) {
        PyErr_Format(PyExc_ValueError, "parseArgs is not a dictionary");
        goto CommonExit;
    }

    // Parse fileNameFieldName
    {
        const char *dictName = "parseArgs";
        valueSuccess =
            extractStringFromDict(parseArgsObj,
                                  dictName,
                                  "fileNameFieldName",
                                  parseArgs_.fileNameFieldName,
                                  sizeof(parseArgs_.fileNameFieldName));
        if (!valueSuccess) {
            goto CommonExit;
        }
        // Check if this is a valid field name
        fieldNameLen = strlen(parseArgs_.fileNameFieldName);
        if (fieldNameLen &&
            !DataPageFieldMeta::fieldNameAllowed(parseArgs_.fileNameFieldName,
                                                 fieldNameLen)) {
            PyErr_Format(PyExc_ValueError,
                         "file name field '%s' is not allowed",
                         parseArgs_.fileNameFieldName);
            goto CommonExit;
        }
    }

    // Parse recordNumFieldName
    {
        const char *dictName = "parseArgs";
        valueSuccess =
            extractStringFromDict(parseArgsObj,
                                  dictName,
                                  "recordNumFieldName",
                                  parseArgs_.recordNumFieldName,
                                  sizeof(parseArgs_.recordNumFieldName));
        if (!valueSuccess) {
            goto CommonExit;
        }
        // Check if this is a valid field name
        fieldNameLen = strlen(parseArgs_.recordNumFieldName);
        if (fieldNameLen &&
            !DataPageFieldMeta::fieldNameAllowed(parseArgs_.recordNumFieldName,
                                                 fieldNameLen)) {
            PyErr_Format(PyExc_ValueError,
                         "record num field '%s' is not allowed",
                         parseArgs_.recordNumFieldName);
            goto CommonExit;
        }
    }

    // Parse schema
    {
        PyObject *fields = PyDict_GetItemString(parseArgsObj, "fields");
        if (!fields) {
            PyErr_Format(PyExc_KeyError, "fields");
            goto CommonExit;
        }

        if (!PyList_Check(fields)) {
            PyErr_Format(PyExc_ValueError, "parseArgs['fields'] is not a list");
            goto CommonExit;
        }

        parseArgs_.fieldNamesCount = PyList_Size(fields);
        for (int ii = 0; ii < PyList_Size(fields); ii++) {
            char dictName[128];
            snprintf(dictName, sizeof(dictName), "parseArgs['fields'][%i]", ii);
            PyObject *field = PyList_GetItem(fields, ii);
            if (!field) {
                PyErr_Format(PyExc_IndexError, "fields index out of range");
                goto CommonExit;
            }

            if (!PyDict_Check(field)) {
                PyErr_Format(PyExc_ValueError,
                             "%s is not a dictionary",
                             dictName);
                goto CommonExit;
            }

            valueSuccess =
                extractStringFromDict(field,
                                      dictName,
                                      "fieldName",
                                      parseArgs_.fieldNames[ii],
                                      sizeof(parseArgs_.fieldNames[ii]));
            if (!valueSuccess) {
                goto CommonExit;
            }

            valueSuccess =
                extractStringFromDict(field,
                                      dictName,
                                      "oldName",
                                      parseArgs_.oldNames[ii],
                                      sizeof(parseArgs_.oldNames[ii]));
            if (!valueSuccess) {
                goto CommonExit;
            }

            PyObject *type = PyDict_GetItemString(field, "type");
            if (!type) {
                goto CommonExit;
            }

            const char *typeStr = PyUnicode_AsUTF8(type);
            if (!typeStr) {
                goto CommonExit;
            }

            parseArgs_.types[ii] = strToDfFieldType(typeStr);
        }
    }

    // Get parser json
    {
        const char *dictName = "parseArgs";
        valueSuccess = extractStringFromDict(parseArgsObj,
                                             dictName,
                                             "parserArgJson",
                                             parseArgs_.parserArgJson,
                                             sizeof(parseArgs_.parserArgJson));
        if (!valueSuccess) {
            goto CommonExit;
        }
    }

    // Parse allowFileErrors
    {
        PyObject *allowFileErrors =
            PyDict_GetItemString(parseArgsObj, "allowFileErrors");
        if (!allowFileErrors) {
            PyErr_Format(PyExc_KeyError, "allowFileErrors");
            goto CommonExit;
        }

        if (!PyBool_Check(allowFileErrors)) {
            PyErr_Format(PyExc_ValueError,
                         "parseArgs['allowFileErrors'] is not a bool");
            goto CommonExit;
        }

        parseArgs_.allowFileErrors = allowFileErrors == Py_True;
    }

    // Parse allowRecordErrors
    {
        PyObject *allowRecordErrors =
            PyDict_GetItemString(parseArgsObj, "allowRecordErrors");
        if (!allowRecordErrors) {
            PyErr_Format(PyExc_KeyError, "allowRecordErrors");
            goto CommonExit;
        }

        if (!PyBool_Check(allowRecordErrors)) {
            PyErr_Format(PyExc_ValueError,
                         "parseArgs['allowRecordErrors'] is not a bool");
            goto CommonExit;
        }

        parseArgs_.allowRecordErrors = allowRecordErrors == Py_True;
    }

    success = true;
CommonExit:
    return success;
}

Status
PyRecordStreamer::err(int64_t recordNum, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    vsnprintf(errorStringBuf_, sizeof(errorStringBuf_), fmt, args);
    va_end(args);

    return reportError(recordNum, errorStringBuf_);
}

Status
PyRecordStreamer::reportFile()
{
    Status status = StatusOk;
    DataPageWriter::PageStatus pageStatus;
    bool success = false;
    int numErrors;

    numErrors_ += fileReport_.numTotalErrors;

    // We want to keep retrying until the errors fit into the page
    numErrors = fileReport_.numErrors;
    do {
        bool triedOnce = false;
        // If we fail to write a page, it's either because we've filled the
        // page full of records, or this record is too big to fit on a page.
        // If it's the former, we want to flush it out and try again. If
        // it's the latter, we want to slim down the record and repeat.
        while (true) {
            DataPageWriter::Record *writeRecord;
            status = packReport(numErrors, &writeRecord);
            BailIfFailed(status);
            assert(writeRecord);

            status = errorWriter_.commit(writeRecord, &pageStatus, NULL);
            BailIfFailed(status);

            if (pageStatus == DataPageWriter::PageStatus::Full) {
                if (triedOnce) {
                    // This means that this set of errors is too large for
                    // a single data page; try again after trimming some off
                    break;
                }
                status = dumpPage(true);
                BailIfFailed(status);

                triedOnce = true;
                continue;
            } else {
                success = true;
                break;
            }
        }
        if (!success) {
            numErrors /= 2;
        }
    } while (!success);

CommonExit:
    return status;
}

Status
PyRecordStreamer::packReport(int64_t numErrors, DataPageWriter::Record **record)
{
    Status status = StatusOk;
    TypedDataValue dValue;
    ProtoFieldValue *pValue;

    status = errorWriter_.newRecord(record);
    BailIfFailed(status);

    // fullPath
    dValue.setString(fileReport_.fullPath, fileReport_.fullPathLen);
    status = (*record)->addFieldByName("fullPath", &dValue);
    BailIfFailed(status);

    // numRecordsInFile
    dValue.setInt64(fileReport_.numRecordsInFile);
    status = (*record)->addFieldByName("numRecordsInFile", &dValue);
    BailIfFailed(status);

    // numTotalErrors
    dValue.setInt64(fileReport_.numTotalErrors);
    status = (*record)->addFieldByName("numTotalErrors", &dValue);
    BailIfFailed(status);

    // sourceArgs.targetName
    dValue.setString(fileReport_.sourceArgs.targetName,
                     strlen(fileReport_.sourceArgs.targetName));
    status = (*record)->addFieldByName("targetName", &dValue);
    BailIfFailed(status);

    // sourceArgs.path
    dValue.setString(fileReport_.sourceArgs.path,
                     strlen(fileReport_.sourceArgs.path));
    status = (*record)->addFieldByName("path", &dValue);
    BailIfFailed(status);

    // sourceArgs.fileNamePattern
    dValue.setString(fileReport_.sourceArgs.fileNamePattern,
                     strlen(fileReport_.sourceArgs.fileNamePattern));
    status = (*record)->addFieldByName("fileNamePattern", &dValue);
    BailIfFailed(status);

    // sourceArgs.recursive
    dValue.setBool(fileReport_.sourceArgs.recursive);
    status = (*record)->addFieldByName("recursive", &dValue);
    BailIfFailed(status);

    // numErrors
    dValue.setInt64(numErrors);
    status = (*record)->addFieldByName("numErrors", &dValue);
    BailIfFailed(status);

    // errors array
    status = pbCreateMessage(&errorArena_, &pValue);
    ProtoFieldValue_ArrayValue *arrayVal;
    status = pbCreateMessage(&errorArena_, &arrayVal);
    BailIfFailed(status);

    // Now we're going to try to add the errors for the current attempt
    for (int ii = 0; ii < numErrors; ii++) {
        ProtoFieldValue *arrElm = arrayVal->add_elements();
        ProtoFieldValue_ObjectValue *objectVal;
        status = pbCreateMessage(&errorArena_, &objectVal);
        BailIfFailed(status);

        const FileError *fe = fileReport_.errors.get(ii);
        status = pbCreateMessage(&errorArena_, &pValue);
        BailIfFailed(status);
        pValue->set_stringval(fe->errStr);

        (*objectVal->mutable_values())["error"] = *pValue;

        status = pbCreateMessage(&errorArena_, &pValue);
        BailIfFailed(status);
        pValue->set_int64val(fe->recNum);

        (*objectVal->mutable_values())["recordNumber"] = *pValue;
        arrElm->set_allocated_objectvalue(objectVal);
    }
    pValue->set_allocated_arrayvalue(arrayVal);
    dValue.setArray(pValue);
    status = (*record)->addFieldByName("errors", &dValue);
    BailIfFailed(status);
    pValue = NULL;

CommonExit:
    return status;
}

Status
PyRecordStreamer::reportError(int64_t recordNum, const char *errStr)
{
    Status status = StatusOk;
    FileError *fe = NULL;
    char *copy;

    PyObject *ret = NULL;

    copy = strAllocAndCopy(errStr);
    BailIfNull(copy);

    fe = new (std::nothrow) FileError(recordNum, copy);
    BailIfNull(fe);

    fileReport_.numTotalErrors++;

    status = fileReport_.errors.append(fe);
    BailIfFailed(status);
    fe = NULL;

    fileReport_.numErrors++;

    ret = PyObject_CallMethod(xceDataset_,
                              "report_error",
                              "Lss",
                              recordNum,
                              fileReport_.fullPath,
                              errStr);
    if (ret == NULL) {
        status = StatusFailed;
        goto CommonExit;
    }

    // If we don't allow record errors, then this error should cause the
    // processing of this file should stop.
    // This should be last here
    if (!parseArgs_.allowRecordErrors) {
        status = StatusRecordError;
    }

CommonExit:
    if (fe) {
        delete fe;
        fe = NULL;
    }
    if (ret != NULL) {
        Py_DECREF(ret);
        ret = NULL;
    }
    return status;
}

Status
PyRecordStreamer::pyExceptionToReport()
{
    assert(PyErr_Occurred());
    Status status = StatusOk;
    UdfError error;

    udfPyErrorCreate(&error, NULL);

    status = err(-1, "%s", error.message_);
    PyErr_Clear();
    BailIfFailed(status);

CommonExit:
    return status;
}

void
PyRecordStreamer::raiseExceptionFromReport()
{
    assert(fileReport_.errors.size() > 0);

    FileError *fe = fileReport_.errors.get(fileReport_.errors.size() - 1);
    assert(fe);
    PyErr_Format(PyExc_RuntimeError, "record: %i, %s", fe->recNum, fe->errStr);
}

Status
PyRecordStreamer::addRecord(int64_t recordNum,
                            DataPageWriter::Record *record,
                            DataPageWriter::PageStatus *pageStatus,
                            int32_t *bytesOverflow)
{
    Status status = StatusOk;

    if (addFileName()) {
        TypedDataValue value;
        value.setString(fileReport_.fullPath, fileReport_.fullPathLen);
        // XXX this is a slow function; O(n) n=number of fields in record;
        // We can probably build some sort of indexing structure to improve
        // this
        if (record->containsField(parseArgs_.fileNameFieldName)) {
            status = err(recordNum,
                         "file name field '%s' present in data",
                         parseArgs_.fileNameFieldName);
            BailIfFailed(status);
        }
        status = record->addFieldByName(parseArgs_.fileNameFieldName, &value);
        BailIfFailed(status);
    }

    if (addRecordNum()) {
        TypedDataValue value;
        // increment so that the record number starts from 1
        int64_t recNum = recordNum + 1;
        value.setInt64(recNum);
        // XXX this is a slow function; O(n) n=number of fields in record;
        // We can probably build some sort of indexing structure to improve
        // this
        if (record->containsField(parseArgs_.recordNumFieldName)) {
            status = err(recordNum,
                         "record number field '%s' present in data",
                         parseArgs_.recordNumFieldName);
            BailIfFailed(status);
        }
        status = record->addFieldByName(parseArgs_.recordNumFieldName, &value);
        BailIfFailed(status);
    }

    status = writer_.commit(record, pageStatus, bytesOverflow);
    BailIfFailed(status);

    if (*pageStatus == DataPageWriter::PageStatus::NotFull) {
        ++fileReport_.numRecordsInFile;
        fileReport_.fileDataSize += record->recordSize();
    }

CommonExit:
    return status;
}

Status
PyRecordStreamer::addRecord(int64_t recordNum, PageWriter::Record *record)
{
    Status status;
    PageWriter::FieldOfInterest *foi = NULL;
    assert(fixedSchemaPageWriter_.isFixedSchema());

    // add record num and file path fields if needed
    foi =
        fixedSchemaPageWriter_.getFieldOfInterest(parseArgs_.fileNameFieldName);
    if (foi) {
        TypedDataValue dValue;
        dValue.setString(fileReport_.fullPath, fileReport_.fullPathLen);
        status = record->setFieldByIndex(foi->idx, &dValue);
        if (!status.ok()) {
            return status;
        }
    }

    foi = fixedSchemaPageWriter_.getFieldOfInterest(
        parseArgs_.recordNumFieldName);
    if (foi) {
        TypedDataValue dValue;
        // increment so that the record number starts from 1
        dValue.setInt64(recordNum + 1);
        status = record->setFieldByIndex(foi->idx, &dValue);
        if (!status.ok()) {
            return status;
        }
    }

    status = fixedSchemaPageWriter_.writeRecord(record);
    if (status.ok()) {
        ++fileReport_.numRecordsInFile;
        fileReport_.fileDataSize += record->recordSize();
    }
    return status;
}

Status
PyRecordStreamer::castFieldValueToProto(DfFieldValue *field,
                                        DfFieldType type,
                                        ProtoFieldValue *protoOut)
{
    Status status = StatusOk;

    try {
        switch (type) {
        case DfInt64:
            protoOut->set_int64val(field->int64Val);
            break;
        case DfFloat64:
            protoOut->set_float64val(field->float64Val);
            break;
        case DfString:
            protoOut->set_stringval(field->stringValTmp);
            break;
        case DfBoolean:
            protoOut->set_boolval(field->boolVal);
            break;
        default:
            assert(0);
            status = StatusUnimpl;
        }
    } catch (std::exception) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    return status;
}

//
// PyRawStreamer
//

PyRawStreamer::~PyRawStreamer()
{
    if (parser_ != NULL) {
        delete parser_;
        parser_ = NULL;
    }
}

bool
PyRawStreamer::init(PyObject *args)
{
    Status status;
    DataFormat *df = DataFormat::get();
    bool success = false;
    PyObject *parseArgsObj;
    PyObject *xceDataset;

    if (!PyArg_ParseTuple(args,
                          "OOlO",
                          &inFileGen_,
                          &parseArgsObj,
                          &formatType_,
                          &xceDataset)) {
        goto CommonExit;
    }
    assert(parseArgsObj != NULL && "guaranteed by ParseTuple");
    assert(inFileGen_ != NULL && "guaranteed by ParseTuple");
    assert(xceDataset != NULL && "guaranteed by ParseTuple");

    // inFileGen arg verification
    if (!PyIter_Check(inFileGen_)) {
        PyErr_Format(PyExc_ValueError, "inFileGen is not a valid iterator");
        goto CommonExit;
    }

    // Let's assume that the xceDataset is valid and fail at runtime
    {
        bool initSuccess = recordStream_.init(xceDataset, parseArgsObj);
        if (!initSuccess) {
            goto CommonExit;
        }
    }

    // Parse format type
    if (!isValidDfFormatType(formatType_)) {
        PyErr_Format(PyExc_ValueError,
                     "parseArgs['format'] (%i) is not a valid format type",
                     formatType_);
        goto CommonExit;
    }

    parser_ = df->getParser(formatType_);
    if (!parser_) {
        PyErr_Format(PyExc_ValueError,
                     "Unable to get a parser for format %s",
                     strGetFromDfFormatType(formatType_));
        goto CommonExit;
    }

    status = parser_->init(recordStream_.parseArgs(),
                           recordStream_.optimizerArgs(),
                           recordStream_.writer(),
                           recordStream_.arena(),
                           &recordStream_);
    if (status != StatusOk) {
        if (!PyErr_Occurred()) {
            PyErr_Format(PyExc_ValueError,
                         "Failed to initialize parser for format %s",
                         strGetFromDfFormatType(formatType_));
        }
        goto CommonExit;
    }

    success = true;
CommonExit:
    return success;
}

bool
PyRawStreamer::run()
{
    Status status;
    bool success = false;

    status = recordStream_.processFiles(inFileGen_, this);
    if (status != StatusOk) {
        goto CommonExit;
    }

    success = true;

CommonExit:
    return success;
}

Status
PyRawStreamer::processFile(const char *fileName, PyObject *inFile)
{
    Status status = StatusOk;
    PyFileReader fileReader;

    status = fileReader.init(inFile);
    if (status != StatusOk) {
        if (!PyErr_Occurred()) {
            PyErr_Format(PyExc_RuntimeError,
                         "failed to initialize file reader");
        }
        goto CommonExit;
    }

    status = parser_->parseData(fileName, &fileReader);
    BailIfFailed(status);

CommonExit:
    return status;
}

// Parses a XPU communication tag string to its enum value.
// NOTE: Keep the strings "ctagBarr" and "ctagUser" in sync with their use
// in python routines like xpuSend_, xpuSendFanout_, and xpuRecv_
Child::XpuCommTag
parseCommTag(const char *str)
{
    if (strncmp("ctagBarr", str, strlen("ctagBarr") + 1) == 0) {
        return Child::XpuCommTag::CtagBarr;
    } else if (strncmp("ctagUser", str, strlen("ctagUser") + 1) == 0) {
        return Child::XpuCommTag::CtagUser;
    }
    return Child::XpuCommTag::CtagInvalid;
}

PyObject *
UdfPyXcalar::getXpuStartIdInNode(PyObject *self, PyObject *args)
{
    unsigned xceNodeId;

    if (!PyArg_ParseTuple(args, "i", &xceNodeId)) {
        xSyslog(pyModuleName,
                XlogErr,
                "UdfPyXcalar::getXpuStartIdInNode failed to parse argument"
                " xceNodeId");
        return NULL;
    }

    if (xceNodeId >= Config::get()->getActiveNodes()) {
        xSyslog(pyModuleName,
                XlogErr,
                "UdfPyXcalar::getXpuStartIdInNode parse argument xceNodeId "
                "%u"
                " is invalid",
                xceNodeId);
        return NULL;
    }

    return PyLong_FromLong(Child::get()->getXpuStartId(xceNodeId));
}

PyObject *
UdfPyXcalar::getXpuEndIdInNode(PyObject *self, PyObject *args)
{
    unsigned xceNodeId;

    if (!PyArg_ParseTuple(args, "i", &xceNodeId)) {
        xSyslog(pyModuleName,
                XlogErr,
                "UdfPyXcalar::getXpuEndIdInNode failed to parse argument"
                " xceNodeId");
        return NULL;
    }

    if (xceNodeId >= Config::get()->getActiveNodes()) {
        xSyslog(pyModuleName,
                XlogErr,
                "UdfPyXcalar::getXpuEndIdInNode parse argument xceNodeId %u"
                " is invalid",
                xceNodeId);
        return NULL;
    }

    return PyLong_FromLong(Child::get()->getXpuEndId(xceNodeId));
}

// Core routine to extract the payload sent by a source XPU, to this
// destination XPU's receive FIFO queue
PyObject *
UdfPyXcalar::xpuRecvString(PyObject *self, PyObject *args)
{
    bool success = false;
    uint8_t *payload = NULL;
    PyObject *retString = NULL;
    char *xpuCommTagStr;
    Child::XpuCommTag commTag;

    commTag = Child::XpuCommTag::CtagBarr;

    if (!PyArg_ParseTuple(args, "s", &xpuCommTagStr)) {
        xSyslog(pyModuleName,
                XlogErr,
                "UdfPyXcalar::xpuRecvString failed to parse argument");
        goto CommonExit;
    }
    if (strcmp(xpuCommTagStr, "ctagBarr") &&
        strcmp(xpuCommTagStr, "ctagUser")) {
        PyErr_Format(PyExc_ValueError,
                     "xpuCommTagStr invalid: %s",
                     xpuCommTagStr);
        xSyslog(pyModuleName,
                XlogErr,
                "UdfPyXcalar::xpuRecvString failed xpuCommTagStr invalid: "
                "%s",
                xpuCommTagStr);
        goto CommonExit;
    }
    commTag = parseCommTag(xpuCommTagStr);
    if (commTag == Child::XpuCommTag::CtagInvalid) {
        PyErr_Format(PyExc_ValueError, "commTag invalid: %d", commTag);
        xSyslog(pyModuleName,
                XlogErr,
                "UdfPyXcalar::xpuRecvString failed commTag invalid: %d",
                commTag);
        goto CommonExit;
    }
    uint64_t payloadLength;
    payload = Child::get()->recvBufferFromLocalXpu(commTag, &payloadLength);
    if (payload) {
        retString = PyBytes_FromStringAndSize((char *) payload, payloadLength);
        if (!retString) {
            PyErr_Format(PyExc_ValueError,
                         "recvBufferFromLocalXpu returned a bad payload");
            xSyslog(pyModuleName,
                    XlogErr,
                    "xpuRecvString- bad payload from "
                    "recvBufferFromLocalXpu!");
            goto CommonExit;
        }
    }
    success = true;
CommonExit:
    if (payload) {
        memFree(payload);
    }
    if (success) {
        // if retString is NULL, this will be a failure. xpuRecv() can't
        // return without returning a valid payload.
        return retString;
    } else {
        assert(PyErr_Occurred());
        return NULL;
    }
}

PyObject *
UdfPyXcalar::xpuComputeNumXdbPagesRequiredOnWire(PyObject *self, PyObject *args)
{
    bool success = false;
    char *payload = NULL;
    int64_t pageSize = 0;
    int payloadLength = 0;
    uint64_t numPagesRequired = 0;

    if (!PyArg_ParseTuple(args, "ly#", &pageSize, &payload, &payloadLength)) {
        goto CommonExit;
    }

    numPagesRequired =
        xpuComputeNumXdbPagesRequiredOnWire(payloadLength, pageSize);

    success = true;
CommonExit:
    if (success) {
        return PyLong_FromLong(numPagesRequired);
    } else {
        assert(PyErr_Occurred());
        return NULL;
    }
}

// Validate arguments and initialize internal state for PySendStringFanout
// class used to implement the _xpu_host.xpuSendStringFanout() method for
// xpuSendFanout()
bool
PySendStringFanout::init(PyObject *args)
{
    bool success = false;
    PyObject *xpuSendListPy_;
    PyObject *dstXpuTuple;

    PyObject *excType = NULL;
    PyObject *excValue = NULL;
    PyObject *excTraceback = NULL;

    if (!PyArg_ParseTuple(args,
                          "slOOOOO!",
                          &xpuCommTagStr_,
                          &pageSize_,
                          &getBuf_,
                          &putBuf_,
                          &putLastBuf_,
                          &sendAllBufs_,
                          &PyList_Type,
                          &xpuSendListPy_)) {
        xSyslog(pyModuleName,
                XlogErr,
                "PySendStringFanout::init failed to parse arguments");
        goto CommonExit;
    }
    assert(getBuf_ != NULL && "guaranteed by ParseTuple");

    if (xpuCommTagStr_ == NULL) {
        PyErr_Format(PyExc_ValueError, "xpuCommTagStr must not be NULL");
        xSyslog(pyModuleName,
                XlogErr,
                "PySendStringFanout::init failed b/c xpuCommTagStr is "
                "NULL");
        goto CommonExit;
    }

    if (strcmp(xpuCommTagStr_, "ctagBarr") &&
        strcmp(xpuCommTagStr_, "ctagUser")) {
        PyErr_Format(PyExc_ValueError,
                     "xpuCommTagStr invalid: %s",
                     xpuCommTagStr_);
        xSyslog(pyModuleName,
                XlogErr,
                "PySendStringFanout::init failed b/c xpuCommTagStr is "
                "invalid: "
                "%s",
                xpuCommTagStr_);
        goto CommonExit;
    }

    xpuCommTag_ = parseCommTag(xpuCommTagStr_);
    if (xpuCommTag_ == Child::XpuCommTag::CtagInvalid) {
        PyErr_Format(PyExc_ValueError, "xpuCommTag invalid: %d", xpuCommTag_);
        xSyslog(pyModuleName,
                XlogErr,
                "PySendStringFanout::init failed xpuCommTag invalid: %d",
                xpuCommTag_);
        goto CommonExit;
    }

    if (pageSize_ <= 0) {
        PyErr_Format(PyExc_ValueError,
                     "pageSize(%ld) must be greater than 0",
                     pageSize_);
        xSyslog(pyModuleName,
                XlogErr,
                "PySendStringFanout::init failed due to invalid pageSize_");
        goto CommonExit;
    }

    // getBuf_ arg verification
    if (!PyCallable_Check(getBuf_)) {
        PyErr_Format(PyExc_ValueError, "getBuf_ is not a valid function");
        xSyslog(pyModuleName,
                XlogErr,
                "PySendStringFanout::init failed because getBuf_ is not a "
                "function");
        goto CommonExit;
    }

    // putBuf_ arg verification
    if (!PyCallable_Check(putBuf_)) {
        PyErr_Format(PyExc_ValueError, "putBuf_ is not a function");
        xSyslog(pyModuleName,
                XlogErr,
                "PySendStringFanout::init failed because putBuf_ not a "
                "function");
        goto CommonExit;
    }

    // putLastBuf_ arg verification
    if (!PyCallable_Check(putLastBuf_)) {
        PyErr_Format(PyExc_ValueError, "putLastBuf_ is not a valid function");
        xSyslog(pyModuleName,
                XlogErr,
                "PySendStringFanout::init failed because putLastBuf_ is "
                "not "
                "a function");
        goto CommonExit;
    }

    // sendAllBufs_ arg verification
    if (!PyCallable_Check(sendAllBufs_)) {
        PyErr_Format(PyExc_ValueError, "sendAllBufs_ is not a valid function");
        xSyslog(pyModuleName,
                XlogErr,
                "PySendStringFanout::init failed because sendAllBufs_ is "
                "not "
                "a function");
        goto CommonExit;
    }

    nXpus_ = PyList_Size(xpuSendListPy_);
    if (nXpus_ == 0) {
        PyErr_Format(PyExc_ValueError,
                     "list of dest XPUs must not be zero length");
        xSyslog(pyModuleName,
                XlogErr,
                "PySendStringFanout::init failed because send list is "
                "empty");
        goto CommonExit;
    }
    messages_ = new (std::nothrow) SingleMessage[nXpus_];
    for (int ii = 0; ii < nXpus_; ii++) {
        dstXpuTuple = PyList_GetItem(xpuSendListPy_, ii);
        if (!PyArg_ParseTuple(dstXpuTuple,
                              "Ly#",
                              &messages_[ii].dstXpuId_,
                              &messages_[ii].payloadString_,
                              &messages_[ii].payloadLength_)) {
            assert(PyErr_Occurred() && "NULL return implies an error");
            PyErr_Fetch(&excType, &excValue, &excTraceback);
            // Add more info to the thrown exception
            if (excValue != NULL && excType != NULL &&
                PyUnicode_Check(excValue) && PyUnicode_AsUTF8(excValue)) {
                PyErr_Clear();
                PyErr_Format(excType,
                             "sendList[%ld]: %s",
                             ii,
                             PyUnicode_AsUTF8(excValue));
            }
            goto CommonExit;
        }

        if (messages_[ii].dstXpuId_ < 0) {
            PyErr_Format(PyExc_ValueError,
                         "%dth dstXpuId(%ld) is negative",
                         ii,
                         messages_[ii].dstXpuId_);
            xSyslog(pyModuleName,
                    XlogErr,
                    "PySendStringFanout::init failed because %dth dstXpuId"
                    "is negative",
                    ii);
            goto CommonExit;
        }
    }
    success = true;
CommonExit:
    if (excType != NULL) {
        Py_DECREF(excType);
        excType = NULL;
    }
    if (excValue != NULL) {
        Py_DECREF(excValue);
        excValue = NULL;
    }
    if (excTraceback != NULL) {
        Py_DECREF(excTraceback);
        excTraceback = NULL;
    }
    return success;
}

uint8_t *
PySendStringFanout::getPageFromObject(PyObject *pageObj)
{
    if (pageObj == NULL) {
        if (!PyErr_Occurred()) {
            PyErr_Format(PyExc_RuntimeError,
                         "Out of resources (Xcalar Managed Memory)");
        }
        return NULL;
    }
    if (!PyLong_Check(pageObj)) {
        PyErr_Format(PyExc_ValueError,
                     "dataset page generator did not provide valid page");
        return NULL;
    }

    PyErr_Clear();
    long pageVal = PyLong_AsLong(pageObj);
    if (PyErr_Occurred()) {
        return NULL;
    }
    return reinterpret_cast<uint8_t *>(pageVal);
}

// The core of the xpuSendStringFanout() method. For each destination XPU
// in the list of destination XPUs, it:
//
//     breaks the dest XPU's payload into a number of buffers obtained via
//     the "getBuf_" routine, by copying either a buffer worth of payload
//     into the buffer, or the remaining payload. For each such buffer, it
//     pushes the buffer using "putBuf_". For the last buffer in the
//     payload, it invokes "putLastBuf_" to which it supplies the
//     destination XPU, the buffer's length (since the payload left may be
//     less than a full buffer's worth), and the total number of buffers for
//     the destination XPU. This routine is expected to also close out the
//     list of buffers for the destination XPU, and append the (dstXpu,
//     buflist) tuple, to the sendList, thus creating the sendList as each
//     destination XPU's payload is processed.
//
// Once all destination XPUs in the supplied xpuSendList have been
// processed, the final "sendAllBufs_" routine is invoked to send the list
// of (destXPU, bufList) tuples (i.e. the sendList) to the XCE parent.  This
// is how the list of (destXPU, payload) tuples supplied to xpuSendFanout()
// is transmitted using SHM/BCMM pages (each page returned by getBuf_ is a
// SHM page). NOTE: In the following, the terms "page" and "buf" are used
// interchangeably.

bool
PySendStringFanout::run()
{
    PyObject *pageObject = NULL;
    char *page;
    const char *myString;
    uint64_t payloadLen;
    unsigned commHdrLen;
    uint64_t payloadPlusHdrLen;
    uint64_t payloadLenInPage;
    uint64_t totLenInPage;
    uint64_t nBufs;
    uint64_t nBufsSave;
    bool success = false;
    char putBufformatStr[] = "l";
    char putLastBufformatStr[] = "llll";
    char sendAllformatStr[] = "l";
    PyObject *ret = NULL;
    int64_t myXpuId = -1;
    bool firstBuf;

    myXpuId = Child::get()->getXpuId();

    for (int ii = 0; ii < nXpus_; ii++) {
        myString = messages_[ii].payloadString_;
        commHdrLen = sizeof(xpuCommTag_);
        payloadLen = messages_[ii].payloadLength_;
        payloadPlusHdrLen = payloadLen + commHdrLen;
        nBufs = UdfPyXcalar::xpuComputeNumXdbPagesRequiredOnWire(payloadLen,
                                                                 pageSize_);
        nBufsSave = nBufs;  // since nBufs is decremented in the loop
        firstBuf = true;
        while (nBufs > 0) {
            pageObject = PyObject_CallFunctionObjArgs(getBuf_, NULL);
            page = (char *) getPageFromObject(pageObject);
            if (page == NULL) {
                xSyslog(pyModuleName,
                        XlogErr,
                        "PySendStringFanout::run failed to get page: "
                        "%lu more pages needed!",
                        nBufs);
                goto CommonExit;
            }
            Py_DECREF(pageObject);
            pageObject = NULL;
            if (firstBuf) {
                // Each payload for a XPU is tagged with the supplied comm
                // tag. This tag is stamped into the first
                // sizeof(xpuCommTag_) bytes of the payload - so the first
                // buffer must carry this tag value. This is stripped out
                // when receiving the payload on the destination, and serves
                // to route the payload to the tag's receive FIFO queue on
                // the destination.
                assert(commHdrLen == sizeof(xpuCommTag_));
                *((Child::XpuCommTag *) page) = xpuCommTag_;
            }
            if (nBufs > 1) {
                assert(!firstBuf || commHdrLen == sizeof(xpuCommTag_));
                // if first buffer, payload is reduced by header size since
                // commHdrLen will be non-zero if this is first buffer
                payloadLenInPage = pageSize_ - commHdrLen;
                memcpy(page + commHdrLen, myString, payloadLenInPage);
                ret = PyObject_CallFunction(putBuf_, putBufformatStr, page);
                if (ret == NULL) {
                    xSyslog(pyModuleName,
                            XlogErr,
                            "PySendStringFanout::run failed to put a page: "
                            "%lu more pages to go!",
                            nBufs);
                    goto CommonExit;
                }
            } else {
                // last buf for this XPU
                payloadLenInPage = payloadLen;
                // totLenInPage logic needed only if firstBuf is true here
                // i.e. nBufsSave == 1, which means both the commHdr and
                // payload fit in a single buffer (firstBuf is true on
                // "last" buf)
                totLenInPage = payloadLenInPage + commHdrLen;
                assert(!firstBuf || commHdrLen == sizeof(xpuCommTag_));
                memcpy(page + commHdrLen, myString, payloadLenInPage);
                ret = PyObject_CallFunction(putLastBuf_,
                                            putLastBufformatStr,
                                            page,
                                            totLenInPage,
                                            messages_[ii].dstXpuId_,
                                            nBufsSave);
                if (ret == NULL) {
                    xSyslog(pyModuleName,
                            XlogErr,
                            "PySendStringFanout::run failed to put last "
                            "page: "
                            "%lu more pages to go!",
                            nBufs);
                    goto CommonExit;
                }
            }
            if (firstBuf) {
                firstBuf = false;
                commHdrLen = 0;
            }
            nBufs--;
            payloadLen -= payloadLenInPage;
            myString += payloadLenInPage;
            assert(nBufs > 0 || payloadLen == 0);
            assert(nBufs == 0 || payloadLen != 0);
            if (nBufs > 0 && payloadLen == 0) {
                xSyslog(pyModuleName,
                        XlogErr,
                        "PySendStringFanout::run for xpu %lu, %dth in list "
                        "failed: NO bytes left but %lu pages remain!",
                        messages_[ii].dstXpuId_,
                        ii,
                        nBufs);
                goto CommonExit;
            }
        }
        if (payloadLen != 0) {
            xSyslog(pyModuleName,
                    XlogErr,
                    "PySendStringFanout::run for xpu %lu, %dth in list "
                    "failed to send entire payload: %lu bytes left!",
                    messages_[ii].dstXpuId_,
                    ii,
                    payloadLen);
            goto CommonExit;
        }
    }
    ret = PyObject_CallFunction(sendAllBufs_, sendAllformatStr, myXpuId);
    if (ret == NULL) {
        xSyslog(pyModuleName,
                XlogErr,
                "PySendStringFanout::run failed to send final sendList");
        goto CommonExit;
    }
    success = true;
CommonExit:
    return success;
}

PyObject *
UdfPyXcalar::xpuSendStringFanout(PyObject *self, PyObject *args)
{
    PySendStringFanout stringSender;

    bool success = stringSender.init(args);
    if (!success) {
        goto CommonExit;
    }
    success = stringSender.run();
    if (!success) {
        goto CommonExit;
    }

CommonExit:
    if (success) {
        Py_RETURN_NONE;
    } else {
        xSyslog(pyModuleName, XlogErr, "xpuSendStringFanout failed!");
        assert(PyErr_Occurred());
        return NULL;
    }
}

PyObject *
UdfPyXcalar::isApp(PyObject *self, PyObject *args)
{
    if (Child::get()->isApp() == true) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}

CsvBuilder::~CsvBuilder()
{
    if (buf_) {
        delete[] buf_;
        buf_ = NULL;
    }
}

Status
CsvBuilder::init(char fieldDelim,
                 char recordDelim,
                 char quoteDelim,
                 DfFormatType formatType)
{
    // XXX We only support single ASCII characters as delimiters
    // because we haven't yet written support for other encodings.
    Status status;
    status = string_.init(InitialStringBufferSize);
    BailIfFailed(status);

    buf_ = new (std::nothrow) char[DfMaxFieldValueSize];
    BailIfNull(buf_);

    fieldDelim_ = fieldDelim;
    recordDelim_ = recordDelim;
    quoteDelim_ = quoteDelim;

    assert(formatType == DfFormatCsv || formatType == DfFormatInternal);
    formatType_ = formatType;

CommonExit:
    return status;
}

Status
CsvBuilder::addField(const DataValueReader *valueReader)
{
    Status status;
    DfFieldValue dfValue;
    DfFieldValue convertedDfValue;
    DfFieldType dfType;
    DFPUtils *dfp = DFPUtils::get();

    const char *stringField;
    int64_t stringFieldLength;
    bool quoteField = false;

    if (!first_) {
        status = string_.append(&fieldDelim_, 1);
        BailIfFailed(status);
    }
    first_ = false;

    dfValue.stringValTmp = buf_;
    status =
        valueReader->getAsFieldValue(DfMaxFieldValueSize, &dfValue, &dfType);
    BailIfFailed(status);

    if (dfType != DfString) {
        // If the type isn't a string, that means we didn't use buf_ for it,
        // so we can use it for the converted type instead
        convertedDfValue.stringValTmp = buf_;

        if (dfType == DfMoney && formatType_ == DfFormatInternal) {
            dfp->xlrNumericToStringInternal(convertedDfValue.stringValTmp,
                                            &dfValue.numericVal,
                                            false);
            assert(convertedDfValue.stringValTmp ==
                   convertedDfValue.stringVal.strActual);
            convertedDfValue.stringVal.strSize =
                strlen(convertedDfValue.stringValTmp) + 1;
        } else {
            // Formats the DfFieldValue src as a string and writes into dst
            status = DataFormat::convertValueType(DfString,
                                                  dfType,
                                                  &dfValue,
                                                  &convertedDfValue,
                                                  DfMaxFieldValueSize,
                                                  BaseCanonicalForm);
            BailIfFailed(status);
        }
        stringFieldLength = convertedDfValue.stringVal.strSize;
        stringField = convertedDfValue.stringVal.strActual;
    } else {
        // strSize contains a null terminator
        stringFieldLength = dfValue.stringVal.strSize;
        stringField = dfValue.stringVal.strActual;
    }
    // Check if we need to quote this string
    if (stringFieldLength == 1) {
        assert(stringField[0] == '\0');

        // this is the empty string, quote it
        quoteField = true;
    }

    for (int ii = 0; ii < stringFieldLength; ii++) {
        char c = stringField[ii];
        if (c == recordDelim_ || c == fieldDelim_ || c == quoteDelim_) {
            quoteField = true;
        }
    }
    if (quoteField) {
        // Start quote
        status = string_.append(&quoteDelim_, 1);
        BailIfFailed(status);
    }
    // strSize contains null terminated character
    for (int ii = 0; ii < stringFieldLength - 1; ii++) {
        char c = stringField[ii];
        // Here we escape quote characters.
        // We don't escape record or field delims, since if they are present
        // the whole string is quoted
        if (c == escapeDelim_) {
            status = string_.append(&escapeDelim_, 1);
            BailIfFailed(status);
        } else if (quoteField && c == quoteDelim_) {
            status = string_.append(&escapeDelim_, 1);
            BailIfFailed(status);
        }
        if (!quoteField) {
            assert(c != recordDelim_);
            assert(c != fieldDelim_);
        }
        status = string_.append(&c, 1);
        BailIfFailed(status);
    }
    if (quoteField) {
        // End quote
        status = string_.append(&quoteDelim_, 1);
        BailIfFailed(status);
    }

CommonExit:
    return status;
}

Status
CsvBuilder::addEmptyField()
{
    Status status;

    if (!first_) {
        status = string_.append(&fieldDelim_, 1);
        BailIfFailed(status);
    }
    first_ = false;

CommonExit:
    return status;
}

Status
CsvBuilder::finishRow(const char **fullRow, int64_t *length)
{
    Status status;
    status = string_.append(&recordDelim_, 1);
    BailIfFailed(status);
    *fullRow = string_.getString();
    *length = string_.size();
CommonExit:
    return status;
}

void
CsvBuilder::clear()
{
    first_ = true;
    string_.clear();
}

//
// SchemaProcessors
//

PyObject *
UdfPyXcalar::findSchema(PyObject *self, PyObject *args)
{
    char *dataType = NULL;
    PyObject *inObj = nullptr;

    // XXX Think about ParseTup'ing everything as PyObjs and giving
    // the user more information when there is a arg type mismatch.
    if (!PyArg_ParseTuple(args, "sO", &dataType, &inObj)) {
        xSyslog(pyModuleName,
                XlogErr,
                "UdfPyXcalar::findSchema failed to parse arguments");
        PyErr_Format(PyExc_TypeError, "Failed to parse function arguments");
        return nullptr;
    }
    if (dataType == nullptr || dataType[0] == '\0') {
        PyErr_Format(PyExc_RuntimeError, "dataType argument was null");
        return nullptr;
    }

    if (!PyBytes_Check(inObj)) {
        PyErr_Format(PyExc_TypeError,
                     "Data must by bytes, not type('%s')",
                     inObj->ob_type->tp_name);
        return nullptr;
    }
    size_t numBytesRead = PyBytes_Size(inObj);
    PyObject *pyNumBytesLong = PyLong_FromSsize_t(numBytesRead);  // new ref
    int64_t dataSize = PyLong_AsLong(pyNumBytesLong);
    Py_DECREF(pyNumBytesLong);

    char *data = nullptr;
    int ret = PyBytes_AsStringAndSize(inObj, (char **) &data, &dataSize);
    if (ret != 0) {
        PyErr_Format(PyExc_RuntimeError, "Failed to read data into buffer");
        return nullptr;
    }

    std::unique_ptr<SchemaProcessor> sp;

    if (strcmp(dataType, "JSON") == 0) {
        sp.reset(new (std::nothrow) JsonSchemaProcessor());
    } else if (strcmp(dataType, "Parquet") == 0) {
        sp.reset(new (std::nothrow) ParquetSchemaProcessor());
    } else {
        PyErr_Format(PyExc_ValueError, "Unknown dataType: '%s'", dataType);
        return nullptr;
    }
    if (sp.get() == nullptr) {
        PyErr_Format(PyExc_RuntimeError, "Failed to allocate SchemaProcessor");
        return nullptr;
    }

    Status status = sp->run(data, dataSize);
    if (status != StatusOk && status != StatusMaxColumnsFound) {
        PyErr_Format(PyExc_RuntimeError,
                     "Failed to run SchemaProcessor: %s",
                     strGetFromStatus(status));
        return nullptr;
    }

    char *jsonOut = nullptr;
    status = sp->jsonifySchema(&jsonOut);
    if (status != StatusOk) {
        PyErr_Format(PyExc_RuntimeError,
                     "Failed to jsonify schema: %s",
                     strGetFromStatus(status));
        return nullptr;
    }

    PyObject *pyJsonOutStr = PyUnicode_FromString(jsonOut);
    if (pyJsonOutStr == nullptr) {
        PyErr_Format(PyExc_RuntimeError,
                     "Failed to get PyUnicode_FromString(json).");
        return nullptr;
    }

    return pyJsonOutStr;
}
