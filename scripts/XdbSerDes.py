# Copyright 2017 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
"""
Usage: Executed within the Xcalar Data Platform

This is a Xcalar Application which is responsible for performing
serialization and deseriailzation of XDB tables
"""

import os
import io
import re
import sys
import json
import ctypes
import logging
import types
from socket import gethostname

import requests
import pywebhdfs
from pywebhdfs.webhdfs import PyWebHdfsClient
from six.moves import http_client

import xcalar.container.parent as xce
import xcalar.container.context as ctx

# Dynamic Constants
hostname = gethostname()

# Set up logging
logger = logging.getLogger('XdbSerDes App Logger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stdout
    thisXpuId = ctx.get_xpu_id()
    thisNodeId = ctx.get_node_id(thisXpuId)
    logHandler = logging.StreamHandler(sys.stdout)
    numNodes = ctx.get_node_count()

    formatter = logging.Formatter(
            '%(asctime)s - Node {} - XdbSerDes App - %(levelname)s - %(message)s'.format(
                thisNodeId))
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)

    logger.debug("XdbSerDes app initialized; nodeId:{}, numNodes:{}, hostname:{}".format(
        thisNodeId,
        numNodes,
        gethostname()
        ))

# Free method will be bound to PyWebHdfsClient instance to allow us to do
# streaming reads from a file handle.  Avoids very large memory allocations and copies.
def getReadFh(self, path):
    uriPathOp = self._create_uri(path, pywebhdfs.operations.OPEN)
    hosts = self._resolve_federation(path)
    assert(len(hosts) == 1)
    uri = uriPathOp.format(host=hosts[0])
    response = requests.get(uri, allow_redirects=True, timeout=self.timeout,
                            stream=True, **self.request_extra_opts)
    if not response.status_code == http_client.OK:
        _raise_pywebhdfs_exception(response.status_code, response.content)

    return response.raw

# Stream to allow us to read directly from an IO vector of shared memory
# addresses.  Allows large atomic HDFS writes while avoiding very large memory
# allocations and copies and avoids use of the broken append_file API.  Note
# there is an asymmetry in requests API that prevents using a writer stream to
# write the IO vector.
class RawIOStream(io.RawIOBase):
    def __init__(self, iterable, hdr):
        self.remainder = hdr
        self.iterable = iterable
    def readable(self):
        return True
    def writable(self):
        return False
    def readinto(self, b):
        try:
            bLen = len(b)
            chunk = self.remainder or next(self.iterable)
            output, self.remainder = chunk[:bLen], chunk[bLen:]
            b[:len(output)] = output
            return len(output)
        except StopIteration:
            return 0

# Iterator over IO vectors of shared memory addresses
class ShmIterable(object):
    def __init__(self, iovs):
        self.iovs = iovs
        self.currIov = iter(self.iovs)

    def __iter__(self):
        return self

    def getShmRef(self, iov):
        size = iov['iov_len']
        addr = iov['iov_base'] + xce.bufCacheOffset
        return (ctypes.c_char * size).from_address(addr)

    def __next__(self):
        # Iteration ends on StopIteration from currIov.next()
        return self.getShmRef(next(self.currIov))

# Minimal SerDes object.  Relies on upper layers for error checking not directly
# related to HDFS IO
class XdbSerDes(object):
    def __init__(self, username, hostname, port, path, fname):
        self.client = PyWebHdfsClient(user_name=username, host=hostname, port=port)
        # Bind the getReadFh free method to our webhdfs instance
        self.client.getReadFh = types.MethodType(getReadFh, self.client, PyWebHdfsClient)

        self.client.make_dir(path, permission=700)
        self.basePath = os.path.normpath(path)
        self.thisXpuId = ctx.get_xpu_id()
        self.thisNodeId = ctx.get_node_id(self.thisXpuId)
        fullPath = os.path.join(self.basePath, hostname, str(self.thisNodeId))
        self.fullPath = os.path.normpath(fullPath)
        self.fqpn = os.path.join(self.fullPath, fname)
        self.fname = fname

    def initCleanup(self):
        try:
            ret = self.client.list_dir(self.fullPath)
        except pywebhdfs.errors.FileNotFound:
            return

        fileStatList = ret['FileStatuses']['FileStatus']
        for fStat in fileStatList:
            if fStat['type'] == 'FILE':
                fname = fStat['pathSuffix']
                if re.match(r'XcalarXdb-.*-.*\.xlrswp', fname):
                    fqpn = os.path.normpath(self.fullPath + '/' + fname)
                    self.client.delete_file_dir(fqpn)

    def serializeIov(self, batchHdr, batchHdrSize, iovs):
        batchHdrPad = batchHdrSize - len(batchHdr)

        assert(batchHdrPad >= 0)
        # Pad header to max base64 size to guarantee constant header field
        # size for deserialization
        padStr = ''.join(['\0'] * batchHdrPad)
        batchHdrPadded = bytes(batchHdr + padStr)

        # Stream will iterate over shared memory IO vectors
        shmIterable = ShmIterable(iovs)
        # XXX: Wrap stream with BufferedIO as requests expects BufferedIO read
        # semantics rather than RawIO read semantics...
        fh = io.BufferedReader(RawIOStream(shmIterable, batchHdrPadded))
        # This call will read vectors directly from shared memory and write to
        # HDFS
        self.client.create_file(self.fqpn, fh, permission=600, overwrite=True)

    def deserializeIov(self, batchHdrSize, iovs):
        fh = self.client.getReadFh(self.fqpn)
        batchHdr = fh.read(batchHdrSize).rstrip('\0')

        for shm in ShmIterable(iovs):
            # Write directly to shared memory
            shm[:] = fh.read(len(shm))

        fh.close()
        self.client.delete_file_dir(self.fqpn)
        return batchHdr

    def drop(self):
        self.client.delete_file_dir(self.fqpn)

def main(inBlob):
    thisXpuId = ctx.get_xpu_id()
    thisNodeId = ctx.get_node_id(thisXpuId)
    logger.debug("Received input: {}".format(inBlob))
    ret = {}
    try:
        inObj = json.loads(inBlob)
        ret['func'] = inObj['func']
        ret['batchHdr'] = ''
        if inObj["func"] == "init":
            serDes = XdbSerDes(inObj['username'], inObj['hostname'],
                                inObj['port'], inObj['path'], '')
            serDes.initCleanup()
        elif inObj["func"] == "serialize":
            serDes = XdbSerDes(inObj['username'], inObj['hostname'],
                                inObj['port'], inObj['path'], inObj['fname'])
            serDes.serializeIov(inObj['batchHdr'], inObj['batchHdrSize'],
                                inObj['iov'])
        elif inObj["func"] == "deserialize":
            serDes = XdbSerDes(inObj['username'], inObj['hostname'],
                                inObj['port'], inObj['path'], inObj['fname'])
            ret['batchHdr'] = serDes.deserializeIov(inObj['batchHdrSize'],
                                inObj['iov'])
        elif inObj["func"] == "drop":
            serDes = XdbSerDes(inObj['username'], inObj['hostname'],
                                inObj['port'], inObj['path'], inObj['fname'])
            serDes.drop()
        else:
            raise ValueError("Not implemented")

    except Exception:
        logger.exception("Node {} caught SerDes exception".format(thisNodeId))
        raise

    return json.dumps(ret)
