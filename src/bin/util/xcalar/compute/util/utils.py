# Copyright 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
from collections import OrderedDict
import json
import math
import time
import requests
import os
import gzip
import zipfile
import tarfile
import sys
import logging
import socket
import random

import http.client
from io import BytesIO
from thrift.transport.TTransport import TTransportException
from contextlib import contextmanager

from xcalar.compute.coretypes.LibApisCommon.ttypes import XcalarApiException
import xcalar.container.xpu_host as xpu_host
from xcalar.external.exceptions import XDPException
from xcalar.compute.coretypes.Status.ttypes import StatusT

def connect_snowflake(cfg):
    import snowflake.connector as sf
    conn = None
    try:
        host = cfg["host"]
        dbname = cfg["dbname"]
        user = cfg["username"]
        schema  = cfg["schema"]
        psw_provider = cfg["psw_provider"]
        psw = cfg["psw_arguments"]
        role = cfg["role"]
        warehouse = cfg["warehouse"]

        # if password is a dict rather than strting -
        # invoke module.method encoded in the dict (e.g. hashivault, psw_provider) to retrieve the password
        if not psw_provider:
            psw = None
        elif psw_provider != "plaintext":
            module = psw_provider.split(".")
            if (len(module) != 2):
                raise Exception(
                    "Module should be of form <modulename.function>")
            exec("import {}".format(module[0]))
            provider = eval(psw_provider)
            psw = provider(psw)

        import snowflake.connector as sf
        conn = sf.connect(
            user=user,
            password=psw,
            account=host,
            database=dbname,
            schema=schema,
            role=role,
            warehouse=warehouse
        )
    except Exception as e:
        raise Exception("Could not connect to Snowflake; Error:{}".format(e))
    return conn

def get_logger(name, log_level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    if not logger.handlers:
        # Logging is not multi-process safe, so stick to stderr
        log_handler = logging.StreamHandler(sys.stderr)
        formatter = logging.Formatter(
            '%(asctime)s - ' + name + ' - %(levelname)s - %(message)s')
        log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)
        logger.debug(name+" initialized; hostname:{}", socket.gethostname())
    return logger

def tar_cz(path):
    """tar workbook directory in memory"""
    file_out = BytesIO()
    tar = tarfile.open(mode="w:gz", fileobj=file_out)
    tar.add(path, arcname="workbook")
    tar.close()
    return file_out.getvalue()


def module_exists(module_name):
    try:
        __import__(module_name)
    except ImportError:
        return False

    return True


class ThriftJSONEncoder(json.JSONEncoder):
    """An encoder that makes python thrift structs JSON serializable via the
  standard python json module.
  Pass this encoder when writing json, like this:
  json.dumps(thriftobj, cls=text.ThriftJSONEncoder, <other kwargs>)
  Note that this is not a full protocol implementation in the thrift sense. This
  is just a quick-and-easy pretty-printer for unittests, debugging etc.
  """
    THRIFT_SPEC = 'thrift_spec'

    def __init__(self, *args, **kwargs):
        super(ThriftJSONEncoder, self).__init__(*args, **kwargs)

    def default(self, o):
        # Handle sets (the value type of a thrift set field). We emit them as lists,
        # sorted by element.
        if isinstance(o, set):
            ret = list(o)
            ret.sort()
            return ret

        # Handle everything that isn't a thrift struct.
        if not hasattr(o, ThriftJSONEncoder.THRIFT_SPEC):
            return super(ThriftJSONEncoder, self).default(o)

        # Handle thrift structs.
        spec = getattr(o, ThriftJSONEncoder.THRIFT_SPEC)
        ret = {}
        for (field_number, type, name, type_info, default) in [
                field_spec for field_spec in spec if field_spec is not None
        ]:
            if name in o.__dict__:
                val = o.__dict__[name]
                if val != default:
                    ret[name] = val
        return ret


def thrift_to_json(o):
    """A utility shortcut function to return a pretty-printed JSON thrift object.
  Map keys will be sorted, making the returned string suitable for use in test comparisons."""
    # ident=2 tells python to pretty-print, and put a newline after each comma. Therefore we
    # set the comma separator to have no space after it. Otherwise it'll be difficult to embed
    # a golden string for comparisons (since our editors strip spaces at the ends of lines).
    return json.dumps(o, cls=ThriftJSONEncoder, separators=(',', ': '))


def build_sorted_node_map(nodes):
    sortedGraph = OrderedDict()
    nodeDict = {}
    for node in nodes:
        nodeDict[node.dagNodeId] = node
    visited_nodes = set()

    def dfs(node):
        visited_nodes.add(node.dagNodeId)
        for pNodeId in node.parents:
            pNode = nodeDict[str(pNodeId)]
            if pNode.dagNodeId not in visited_nodes:
                dfs(pNode)
        sortedGraph[node.dagNodeId] = node

    for nodeId, node in nodeDict.items():
        if node.dagNodeId not in visited_nodes:
            dfs(node)
    return sortedGraph


def get_proto_field_value(pfv):
    """ Get nested ProtoFieldValue from ProtoRow in ProtoFieldValue.proto
    """
    type_str = pfv.WhichOneof("dataValue")
    if type_str is None:
        return None
    data_val = getattr(pfv, type_str)
    if type_str == 'arrayValue':
        val = list(map(get_proto_field_value, data_val.elements))
    elif type_str == 'objectValue':
        val = dict()
        for k, v in data_val.values.items():
            val[k] = get_proto_field_value(v)
    elif type_str == 'timeVal':
        val = data_val.ToJsonString()
    elif type_str == 'numericVal':
        val = xpu_host.convert_money_to_string(data_val.val[0],
                                               data_val.val[1])
    else:
        val = data_val
    return val


# Used to report skew in top tool and in a dataflow import UDF in default.py
# The same logic is used by XD to report skew
def get_skew(num_rows_per_node):
    skew = 0
    num_cluster_nodes = len(num_rows_per_node)
    total_row_count = sum(num_rows_per_node)

    if total_row_count <= 1 or num_cluster_nodes <= 1:
        # if total rows is < 1, no skew
        return skew

    even = 1 / num_cluster_nodes
    for nrows in num_rows_per_node:
        skew += abs(nrows / total_row_count - even)
    # worst case when all rows are on one node, the
    # above loop results in (2 * (N - 1))/N - so
    # multiply this by N/(2 * (N - 1) to get a 1
    # for the worst case
    skew = skew * num_cluster_nodes / (2 * (num_cluster_nodes - 1))
    skew = math.floor(skew * 100)
    return skew


# XXX need to move all utils in this module and in other utilies under this class
class XcUtil(object):

    # retry function defaults
    MAX_RETRIES = 15
    RETRY_TIMEOUT = 1
    # adding TTransportException, thrift exception commonly seen
    RETRY_EXCEPTIONS = [requests.exceptions.Timeout, TTransportException]
    # adding StatusApisWorkTooManyOutstanding as XCE has an admission control limit
    RETRY_XC_STATUS_CODES = [StatusT.StatusApisWorkTooManyOutstanding]

    # retries function func applying args and kwargs passed
    # for list of retry_exceptions(any python exceptions) or
    # list of xcalar status codes.
    @staticmethod
    def retryer(func,
                *args,
                retry_exceptions=RETRY_EXCEPTIONS,
                retry_xc_status_codes=RETRY_XC_STATUS_CODES,
                max_retries=MAX_RETRIES,
                retry_sleep_timeout=RETRY_TIMEOUT,
                **kwargs):
        """
        :param func: *Required*. generic function to invoke.
        :type func: function
        :param retry_exceptions: List of python exceptions (sub class of python Exception)
                                for which we want to retry func call.
        :type retry_exceptions: list
        :param retry_xc_status_codes: List of xcalar status codes for which we want to retry func.
        :type retry_xc_status_codes: list
        :param max_retries: Maximum number of retries to make before giveup.
        : type max_retries: int
        : param retry_sleep_timeout: Time in secs to sleep before retry.
        :type retry_sleep_timeout: int
        """
        for retry in range(0, max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as ex:
                # we reached end of max_retries, fail the operation
                # with the last failure
                if retry == max_retries:
                    raise
                if type(ex) in retry_exceptions or XcUtil.get_xc_status_code(
                        ex) in retry_xc_status_codes:
                    # sleep and retry
                    time.sleep(retry_sleep_timeout)
                else:
                    raise

    # We have two xcalar exception wrappers for xcalar api call errors
    # XcalarApiException(LegacyApis raise this class of exceptions)
    # XDPException (Proto apis raise this exception)
    @staticmethod
    def get_xc_status_code(xc_exception):
        if isinstance(xc_exception, XcalarApiException):
            return xc_exception.status
        elif isinstance(xc_exception, XDPException):
            return xc_exception.statusCode
        else:
            # xc_exception is not a valid xcalar wrapper exception
            return -1

    @contextmanager
    def ignored(*exceptions):
        try:
            yield
        except exceptions:
            pass

    @contextmanager
    def ignored_xc_status_codes(*xc_status_codes):
        try:
            yield
        except Exception as ex:
            xc_code = XcUtil.get_xc_status_code(ex)
            if xc_code not in xc_status_codes:
                raise


class FileUtil(object):
    class UnPackers(object):
        @staticmethod
        def unpack_tar(tarfilename, open_file):
            try:
                seekable = open_file.seekable()
            except AttributeError:
                seekable = False
            if not seekable:
                raise FileUtil.SeekableError("Not Seekable")
            tar = tarfile.TarFile(fileobj=open_file)
            for fname in tar.getnames():
                f = tar.extractfile(fname)
                finfo = tar.getmember(fname)
                if not finfo.isfile() and not finfo.issym():
                    continue
                # Note that 'f.name' is the name of the archive and is not the
                # same as 'fname' even though 'f' is a file object and will
                # return the content of 'fname'.  This is the reason we can't
                # just pass around open_file.
                yield (fname, f, finfo.size)

        @staticmethod
        def unpack_zip(zipfilename, open_file):
            try:
                seekable = open_file.seekable()
            except AttributeError:
                seekable = False
            if not seekable:
                raise FileUtil.SeekableError("Not Seekable")
            with zipfile.ZipFile(open_file) as zf:
                for fname in zf.namelist():
                    zip_info = zf.getinfo(fname)
                    if not zip_info.is_dir():
                        with zf.open(fname, 'r') as f:
                            yield (fname, f, zip_info.file_size)

        @staticmethod
        def unpack_gzip(gzipfilename, open_file):
            with gzip.GzipFile(fileobj=open_file) as gz:
                # Don't have a way to get the size
                yield (gzipfilename[:-3], gz, 0)

    UnPackersMap = {
        ".tar": UnPackers.unpack_tar,
        ".zip": UnPackers.unpack_zip,
        ".gz": UnPackers.unpack_gzip
    }

    @staticmethod
    def unpack_file(filename, openfile, filesize):
        # Get the file extension if there is one and see if there's
        # an associated unpacker
        unpacker = FileUtil.UnPackersMap.get(
            FileUtil.file_extension(filename), None)
        if unpacker:
            for subfilename, opensubfile, filesize in unpacker(
                    filename, openfile):
                # Each of the subfiles could itself need unpacking
                for sfile, filesize in FileUtil.unpack_file(
                        subfilename, opensubfile, filesize):
                    yield (sfile, filesize)
        else:
            # Not a packed file
            yield (openfile, filesize)

    class SeekableError(ValueError):
        pass

    @staticmethod
    def file_extension(filename):
        fname, fext = os.path.splitext(filename.lower())
        return fext

    def isFiledPacked(filename):
        return FileUtil.file_extension(filename) in FileUtil.UnPackersMap
