# Copyright 2016-2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import logging

import xcalar.compute.localtypes.ProtoMsg_pb2 as _ProtoMsg
import xcalar.compute.localtypes.ParentChild_pb2 as _ParentChild
from xcalar.external.client import Client as XdpClient

import xcalar.container.xpu_host as xpu_host

logger = logging.getLogger("xcalar")

#
# Types.
#


class XcalarException(Exception):
    def __init__(self, status, message):
        self.message = message
        self.status = status

    def __str__(self):
        return "%d: %s" % (self.status, self.message)


class Client(XdpClient):
    def __init__(self, user_name=None):
        if not xpu_host.is_xpu():
            raise ValueError("Not running in XPU process")
        XdpClient.__init__(self, bypass_proxy=True, user_name=user_name)
        self.xce_socket_path = xpu_host.get_socket_path()
        self.buf_cache = xpu_host.get_buf_cache_base_addr()
        self.child_id = xpu_host.get_child_id()
        self.xdb_page_size = xpu_host.get_xdb_page_size()

    #
    # Utility methods.
    #

    def _init_msg(self, target):
        msg = _ProtoMsg.ProtoMsg()
        msg.type = _ProtoMsg.ProtoMsgTypeRequest

        self.next_request_id_lock.acquire()
        msg.request.requestId = self.next_request_id
        self.next_request_id += 1
        self.next_request_id_lock.release()

        msg.request.childId = self.child_id
        msg.request.target = target

        return msg

    #
    # Public App SDK.
    #

    def get_version(self):
        msg = self._init_msg(_ProtoMsg.ProtoMsgTargetApi)
        msg.request.api.func = _ProtoMsg.ApiFuncGetVersion
        msg_response = self._send_msg(msg)
        if msg_response.response.status != 0:
            raise XcalarException(msg_response.response.status,
                                  msg_response.response.error)
        return msg_response.response.api.version

    def get_usrnode_buf_cache_addr(self):
        msg_request = self._init_msg(_ProtoMsg.ProtoMsgTargetApi)
        msg_request.request.api.func = _ProtoMsg.ApiFuncGetBufCacheAddr
        msg_response = self._send_msg(msg_request)
        if msg_response.response.status != 0:
            raise XcalarException(msg_response.response.status,
                                  msg_response.response.error)
        return msg_response.response.api.addr

    def get_xdb_page_size(self):
        return self.xdb_page_size

    def get_node_id(self):
        msg_request = self._init_msg(_ProtoMsg.ProtoMsgTargetParent)
        msg_request.request.parent.func = _ParentChild.ParentFuncAppGetNodeId
        msg_response = self._send_msg(msg_request)
        if msg_response.response.status != 0:
            raise XcalarException(msg_response.response.status,
                                  msg_response.response.error)
        return int(msg_response.response.parentChild.nodeId)

    def get_node_count(self):
        msg_request = self._init_msg(_ProtoMsg.ProtoMsgTargetParent)
        msg_request.request.parent.func = _ParentChild.ParentFuncAppGetNodeCount
        msg_response = self._send_msg(msg_request)
        if msg_response.response.status != 0:
            raise XcalarException(msg_response.response.status,
                                  msg_response.response.error)
        return int(msg_response.response.parentChild.nodeCount)

    def get_output_buffers(self, num_buffers):
        msg_request = self._init_msg(_ProtoMsg.ProtoMsgTargetParent)
        msg_request.request.parent.func = _ParentChild.ParentFuncAppGetOutputBuffer
        msg_request.request.parent.app.getBuffers.numBuffers = num_buffers

        msg_response = self._send_msg(msg_request)
        if msg_response.response.status != 0:
            logger.error(
                f'get output buffers failed with status {msg_response.response.status} '
                f'and error {msg_response.response.error}')
            raise XcalarException(msg_response.response.status,
                                  msg_response.response.error)
        buf_addrs = [
            self.buf_cache + buf.offset
            for buf in msg_response.response.parentChild.outputBuffers.bufs
        ]
        return buf_addrs

    def load_output_buffers(self,
                            num_files,
                            num_file_bytes,
                            num_errors,
                            bufs,
                            empty_bufs,
                            fixed_schema=False):
        """bufs must be a tuple of (address, is_error)"""
        msg_request = self._init_msg(_ProtoMsg.ProtoMsgTargetParent)
        msg_request.request.parent.func = _ParentChild.ParentFuncAppLoadBuffer
        msg_request.request.parent.app.loadBuffers.numFiles = num_files
        msg_request.request.parent.app.loadBuffers.numFileBytes = num_file_bytes
        msg_request.request.parent.app.loadBuffers.numErrors = num_errors
        msg_request.request.parent.app.loadBuffers.fixedSchema = fixed_schema
        for buf in bufs:
            new_buffer = msg_request.request.parent.app.loadBuffers.dataBuffers.add(
            )
            new_buffer.hasErrors = buf[1]
            new_buffer.buffer.offset = buf[0] - self.buf_cache

        for buf in empty_bufs:
            new_buffer = msg_request.request.parent.app.loadBuffers.unusedBuffers.add(
            )
            new_buffer.offset = buf - self.buf_cache

        msg_response = self._send_msg(msg_request)
        if msg_response.response.status != 0:
            logger.error(
                f'load output buffers failed with status {msg_response.response.status} '
                f'and error {msg_response.response.error}')
            raise XcalarException(msg_response.response.status,
                                  msg_response.response.error)
        return

    def get_group_id(self):
        msg_request = self._init_msg(_ProtoMsg.ProtoMsgTargetParent)
        msg_request.request.parent.func = _ParentChild.ParentFuncAppGetGroupId
        msg_response = self._send_msg(msg_request)
        if msg_response.response.status != 0:
            raise XcalarException(msg_response.response.status,
                                  msg_response.response.error)
        return msg_response.response.parentChild.groupId

    def report_num_files(self, files_sampled, total_file_bytes, down_sampled):
        msg_request = self._init_msg(_ProtoMsg.ProtoMsgTargetParent)
        msg_request.request.parent.func = _ParentChild.ParentFuncAppReportNumFiles
        msg_request.request.parent.app.reportNumFiles.filesSampled = files_sampled
        msg_request.request.parent.app.reportNumFiles.totalFileBytes = total_file_bytes
        msg_request.request.parent.app.reportNumFiles.downSampled = down_sampled

        msg_response = self._send_msg(msg_request)
        if msg_response.response.status != 0:
            raise XcalarException(msg_response.response.status,
                                  msg_response.response.error)

    def report_file_error(self, file_name, file_error):
        msg_request = self._init_msg(_ProtoMsg.ProtoMsgTargetParent)
        msg_request.request.parent.func = _ParentChild.ParentFuncAppReportFileError
        msg_request.request.parent.app.fileError.fileName = file_name
        msg_request.request.parent.app.fileError.fileError = file_error

        msg_response = self._send_msg(msg_request)
        if msg_response.response.status != 0:
            raise XcalarException(msg_response.response.status,
                                  msg_response.response.error)

    #
    # Part of the XPU <-> XPU transmission channel. This child->parent transmission
    # routine sends a list of [ (dst_xpu, buf_list) ] tuples, with buf_list being a
    # list of [addr, length] tuples - to src_xpu_id's XCE parent, so that the XCE
    # parent can route the payload to all the destinations.  The "addr" must be
    # converted to an offset in the child's SHM segment (which starts at the VA
    # buf_cache) - which will be converted by XCE to an addr in the parent's
    # address space by adding the offset to the VA of the SHM segment in the XCE
    # parent's address space.
    #
    def send_output_buffers_fanout(self, src_xpu_id, send_list, unused_list):
        """send_list must be a list of (dest_xpu, buf_list) tuples"""
        """buf_list must be a list of (addr, len) tuples"""
        """unused_list must be a list of addr"""
        msg_request = self._init_msg(_ProtoMsg.ProtoMsgTargetParent)
        msg_request.request.parent.func = _ParentChild.ParentFuncXpuSendListToDsts
        msg_request.request.parent.app.sendListToDsts.srcXpuId = src_xpu_id
        for dest in send_list:
            new_dest = msg_request.request.parent.app.sendListToDsts.sendList.add(
            )
            new_dest.dstXpuId = dest[0]
            for buf in dest[1]:
                new_buffer = new_dest.buffers.add()
                new_buffer.offset = buf[0] - self.buf_cache
                new_buffer.length = buf[1]

        for buf in unused_list:
            new_buf = msg_request.request.parent.app.sendListToDsts.unusedList.add(
            )
            new_buf.offset = buf - self.buf_cache

        msg_response = self._send_msg(msg_request)
        if msg_response.response.status != 0:
            raise XcalarException(msg_response.response.status,
                                  msg_response.response.error)
        return

    def xdb_get_meta(self, xdb_id):
        msg_request = self._init_msg(_ProtoMsg.ProtoMsgTargetParent)
        msg_request.request.parent.func = _ParentChild.ParentFuncXdbGetMeta
        msg_request.request.parent.app.xdbGetMeta.xdbId = xdb_id

        msg_response = self._send_msg(msg_request)
        if msg_response.response.status != 0:
            raise XcalarException(msg_response.response.status,
                                  msg_response.response.error)

        return msg_response.response.parentChild.xdbGetMeta

    def xdb_get_local_rows(self,
                           xdb_id,
                           columns,
                           num_rows,
                           node_id,
                           start_row=0,
                           as_data_page=False):
        msg_request = self._init_msg(_ProtoMsg.ProtoMsgTargetParent)
        msg_request.request.parent.func = _ParentChild.ParentFuncXdbGetLocalRows
        msg_request.request.parent.app.xdbGetLocalRows.xdbId = xdb_id
        msg_request.request.parent.app.xdbGetLocalRows.columns.extend(columns)
        msg_request.request.parent.app.xdbGetLocalRows.startRow = start_row
        msg_request.request.parent.app.xdbGetLocalRows.numRows = num_rows
        msg_request.request.parent.app.xdbGetLocalRows.nodeId = node_id
        msg_request.request.parent.app.xdbGetLocalRows.asDataPage = as_data_page

        msg_response = self._send_msg(msg_request)
        if msg_response.response.status != 0:
            raise XcalarException(msg_response.response.status,
                                  msg_response.response.error)

        return msg_response.response.parentChild.xdbGetLocalRows
