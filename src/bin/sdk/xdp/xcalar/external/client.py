# Copyright 2018 - 2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import json
import os
import re
import hashlib
import base64
import requests
import random
import socket
import errno
import struct
import threading
import time
import logging

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from urllib.parse import urlparse, urlunparse, urljoin

from xcalar.external.LegacyApi.WorkItem import (
    WorkItemGetNumNodes, WorkItemGetStats, WorkItemGetStatGroupIdMap,
    WorkItemGetConfigParams, WorkItemSetConfigParam, WorkItemSessionList,
    WorkItemSessionNew, WorkItemSessionDelete, WorkItemSessionActivate,
    WorkItemSessionUpload, WorkItemAddTarget2, WorkItemListTargets2,
    WorkItemListTargetTypes2, WorkItemGetDatasetsInfo, WorkItemUdfAdd,
    WorkItemListXdfs)
from xcalar.external.LegacyApi.XcalarApi import XcalarApi as LegacyXcalarApi
from xcalar.external.LegacyApi.XcalarApi import (
    XcalarApiStatusException as LegacyXcalarApiStatusException)
from xcalar.external.kvstore import KvStore
from xcalar.external.Retina import Retina
from xcalar.external.LegacyApi.Udf import Udf
from xcalar.external.LegacyApi.AuthUtil import Authenticate
from xcalar.external.LegacyApi.Env import XcalarServiceXceUrl

from xcalar.external.data_target import DataTarget
from xcalar.external.driver import Driver
from xcalar.external.dataset import Dataset
from xcalar.external.app import App
from xcalar.external.exceptions import XDPException
import xcalar.external.workbook

import xcalar.external.session
import xcalar.external.kvstore
import xcalar.external.runtime

from xcalar.compute.localtypes.Service_pb2 import ServiceRequest
from xcalar.compute.localtypes.ProtoMsg_pb2 import ProtoMsg, ProtoMsgTypeRequest, ProtoMsgTargetService
from xcalar.compute.localtypes.License_pb2 import CreateRequest
from xcalar.compute.localtypes.License_pb2 import DestroyRequest
from xcalar.compute.localtypes.License_pb2 import GetRequest
from xcalar.compute.localtypes.License_pb2 import UpdateRequest
from xcalar.compute.localtypes.License_pb2 import ValidateRequest
from xcalar.compute.localtypes.Stats_pb2 import GetLibstatsRequest, ResetStatsRequest
from xcalar.compute.localtypes.Table_pb2 import ListTablesRequest

from xcalar.compute.localtypes.Workbook_pb2 import WorkbookScope
from xcalar.compute.coretypes.LibApisCommon.ttypes import XcalarApiWorkbookScopeT
from xcalar.compute.coretypes.Status.ttypes import StatusT

from xcalar.external.LogLevel import XcalarSyslogMsgLevel
from xcalar.external.LogLevel import XcalarSyslogFlushLevel

from xcalar.compute.services.App_xcrpc import App as AppClient
from xcalar.compute.services.Cgroup_xcrpc import Cgroup as CgroupClient
from xcalar.compute.services.Dataflow_xcrpc import Dataflow as DataflowClient
from xcalar.compute.services.Connectors_xcrpc import Connectors as ConnectorsClient
from xcalar.compute.services.KvStore_xcrpc import KvStore as KvStoreClient
from xcalar.compute.services.License_xcrpc import License as LicenseClient
from xcalar.compute.services.log_xcrpc import Log as LogClient
from xcalar.compute.services.memory_xcrpc import Memory as MemoryClient
from xcalar.compute.services.PublishedTable_xcrpc import PublishedTable as PublishedTableClient
from xcalar.compute.services.Query_xcrpc import Query as QueryClient
from xcalar.compute.services.ResultSet_xcrpc import ResultSet as ResultSetClient
from xcalar.compute.services.Sql_xcrpc import Sql as SqlClient
from xcalar.compute.services.Stats_xcrpc import Stats as StatsClient
from xcalar.compute.services.Table_xcrpc import Table as TableClient
from xcalar.compute.services.UDF_xcrpc import UserDefinedFunction as UDFClient
from xcalar.compute.services.Version_xcrpc import Version as VersionClient
from xcalar.compute.services.Workbook_xcrpc import Workbook as WorkbookClient

#
# Grpc stubs
#
# from xcalar.external.services.app_grpc import AppClient
# from xcalar.external.services.cgroup_grpc import CgroupClient
# from xcalar.external.services.dataflow_grpc import DataflowClient
# from xcalar.external.services.file_grpc import FileClient
# from xcalar.external.services.kv_store_grpc import KvStoreClient
# from xcalar.external.services.license_grpc import LicenseClient
# from xcalar.external.services.log_grpc import LogClient
# from xcalar.external.services.memory_grpc import MemoryClient
# from xcalar.external.services.published_table_grpc import PublishedTableClient
# from xcalar.external.services.query_grpc import QueryClient
# from xcalar.external.services.result_set_grpc import ResultSetClient
# from xcalar.external.services.sql_grpc import SqlClient
# from xcalar.external.services.stats_grpc import StatsClient
# from xcalar.external.services.table_grpc import TableClient
# from xcalar.external.services.udf_grpc import UDFClient
# from xcalar.external.services.version_grpc import VersionClient
# from xcalar.external.services.workbook_grpc import WorkbookClient
import xcalar.compute.localtypes.memory_pb2 as memory_pb2
import xcalar.compute.localtypes.log_pb2 as log_pb2
import xcalar.compute.localtypes.App_pb2 as App_pb2

import xcalar.container.xpu_host as xpu_host

from google.protobuf.json_format import MessageToJson
import xcalar.external.udf
from google.protobuf.empty_pb2 import Empty

logger = logging.getLogger()

# dataflows are always owned by a special user (same one that XD uses)
# XXX: In the future, it'd be good to push these APIs into XCE, so a
# special user-name isn't exposed to either XD or pyClient and such
# dependencies can be avoided. XCE can use sessions with fake
# user-name behind these APIs, but this wouldn't be exposed to the
# consumer - to the API consuming layer, it'd just be dataflows and
# dataflow files.
dataflow_user_name = ".xcalar.published.df"
dataflow_user_id = 1
dataflow_optimized_execution_key = "DF2Optimized"
sys_wkbk_name = ".system_workbook_"


def _recvall(sock, num_bytes):
    """recv len(destbuf) data into destbuf"""
    # This receives repeated packets until we have received the full message.
    # it could probably be improved with bytearray and memoryview to fully
    # preallocate the destination buffer
    full_data = b''
    max_retries = 1000
    retries_left = max_retries
    while len(full_data) < num_bytes:
        remaining_bytes = num_bytes - len(full_data)
        try:
            packet = sock.recv(remaining_bytes)
            retries_left = max_retries
        except socket.error as err:
            retries_left -= 1
            if retries_left > 0 and (err.errno == errno.EAGAIN
                                     or err.errno == errno.EWOULDBLOCK):
                time.sleep(0.1)
                continue
            else:
                raise
        if not packet:
            raise RuntimeError(
                "Incomplete read; only received {} bytes of expected {}".
                format(len(full_data), num_bytes))
        full_data += packet
    return full_data


class PendingRequest():
    def __init__(self, lock, response_msg=None):
        self.response_msg = response_msg
        self._condition = threading.Condition(lock)

    def wait(self):
        self._condition.wait()

    def notify(self):
        self._condition.notify()


class Client():
    """
    The Xcalar Api client is the starting point for working with the
    Xcalar Data Platform SDK. A client class represents a user in Xcalar and
    thus all objects that are shared/global for a user
    (e.g Workbooks, Datasets, UDF modules and DataTargets)
    should be accessed from the client class.

    To obtain a Client instance:
        >>> from xcalar.external.client import Client
        >>> client = Client()

    **Attributes**

    The following attributes are properties associated with
    the Xcalar Api Client. They cannot be directly set.

    * **username** (:class:`str`) - Unique name to identify the user.

    **Methods**

    These are the available methods:

    * :meth:`get_version`
    * :meth:`get_config_params`
    * :meth:`set_config_param`
    * :meth:`get_num_nodes_in_cluster`
    * :meth:`get_metrics_from_node`
    * :meth:`reset_libstats`
    * :meth:`reset_metrics_on_node`
    * :meth:`get_log_level`
    * :meth:`set_log_level`
    * :meth:`get_memory_usage`

     **Note**: *The Client class contains operations that create, get, upload,
     list, and destroy
     workbooks. All other methods to operate on a Workbook object
     are in the* :class:`Workbook <xcalar.external.workbook.Workbook>` *class*.

    * :meth:`create_workbook`
    * :meth:`upload_workbook`
    * :meth:`get_workbook`
    * :meth:`list_workbooks`
    * :meth:`destroy_workbook`

     **Note**: *The Client class contains operations that add, get and list
     data targets. All other methods to operate on a DataTarget object
     are in
     the* :class:`DataTarget <xcalar.external.data_target.DataTarget>` *class*.

    * :meth:`add_data_target`
    * :meth:`get_data_target`
    * :meth:`list_data_targets`

     **Note**: *The Client class contains operations that get and list dataset.
     All other methods to operate on a Dataset object
     are in the* :class:`Dataset <xcalar.external.dataset.Dataset>` *class.
     In order to import a new dataset, the*
     :meth:`build_dataset <xcalar.external.workbook.Workbook.build_dataset>`
     *method should be used*.

    * :meth:`get_dataset`
    * :meth:`list_datasets`

     **Note**: *The Client class contains operations that create, list, execute,
     and execute dataflows*.

    * :meth:`add_dataflow`
    * :meth:`get_dataflow`
    * :meth:`execute_dataflow`
    * :meth:`delete_dataflow`
    * :meth:`list_dataflows`

     **Note**: *The Client class contains operations that create, get, validate,
     delete, and update licenses*.

    * :meth:`create_license`
    * :meth:`get_license`
    * :meth:`validate_license`
    * :meth:`update_license`
    * :meth:`delete_license`

     **Note**: *The Client class contains operations that create, get, list,
     and destroy sessions*.

    * :meth:`create_session`
    * :meth:`get_session`
    * :meth:`list_sessions`
    * :meth:`destroy_session`

     **Note**: *The Client class contains operations that create, get, and list
     shared UDF modules (i.e. shared between workbooks and users), returning
     UDF class object(s). Similar operations for UDF modules in the workbook
     are in the* :class:`Workbook <xcalar.external.workbook.Workbook>` *class*.
     The methods to operate on the returned UDF class object are in
     the :class:`UDF <xcalar.external.udf.UDF>` *class*.

    * :meth:`create_udf_module`
    * :meth:`get_udf_module`
    * :meth:`list_udf_modules`

    |br|

    .. testsetup::

        from xcalar.external.client import Client
        client = Client()

    .. testcleanup::

        client.get_workbook("get_demo")
        client.get_workbook("upload_demo")
        client.get_workbook("create_demo")
        client.get_workbook("download_workbook")

    """

    def __init__(self,
                 url=None,
                 client_secrets=None,
                 client_secrets_file=None,
                 client_token=None,
                 bypass_proxy=False,
                 user_name=None,
                 session_type='api',
                 save_cookies=False):
        """
        Client to connect to a Xcalar cluster

        :param url: Url of the Xcalar cluster, like "https://my-cluster:12124"
        :type url: str
        :param client_secrets_file:
            (Experimental) secrets file for authentication
        :type client_secrets_file: str
        """
        self.auth = Authenticate(
            url=url,
            client_secrets=client_secrets,
            client_secrets_file=client_secrets_file,
            bypass_proxy=bypass_proxy,
            client_token=client_token,
            session_type=session_type,
            save_cookies=save_cookies)
        self._legacy_xcalar_api = LegacyXcalarApi(
            auth_instance=self.auth, sdk_client=self)
        self._bypass_proxy = bypass_proxy
        if self.auth.username:
            self._user_name = self.auth.username
        elif user_name:
            self._user_name = user_name
        else:
            self._user_name = "admin"

        self.xce_sockets = {}

        self.next_request_id = 0
        self.next_request_id_lock = threading.Lock()

        self._session = requests.Session()
        retry = Retry(
            total=7,
            backoff_factor=0.5,
            method_whitelist=('GET', 'POST'),
            status_forcelist=(502, ))    # retry for 60s
        self._session.mount('http://', HTTPAdapter(max_retries=retry))
        if self.auth._using_proxy:
            # building the proxy url using service endpoint url path
            service_url = self.auth.parsed_url._replace(
                path=urljoin("/app/",
                             urlparse(XcalarServiceXceUrl).path.strip("/")))
            self._url = urlunparse(service_url)
        else:
            self._url = XcalarServiceXceUrl
        # XXX: Need to address this in https://xcalar.atlassian.net/browse/SDK-29
        self.user_id = (int(
            hashlib.md5(self._user_name.encode("UTF-8")).hexdigest()[:5], 16) +
                        4000000)
        self._config_params = None
        self._data_target_types = None
        self._retina = Retina(self._legacy_xcalar_api)
        self._udf = Udf(self._legacy_xcalar_api)

        self._app_service = AppClient(self)
        self._cgroup_service = CgroupClient(self)
        self._connectors_service = ConnectorsClient(self)
        self._dataflow_service = DataflowClient(self)
        self._kvstore_service = KvStoreClient(self)
        self._license_service = LicenseClient(self)
        self._log_service = LogClient(self)
        self._memory_service = MemoryClient(self)
        self._published_table_service = PublishedTableClient(self)
        self._query_service = QueryClient(self)
        self._result_set_service = ResultSetClient(self)
        self._sql_service = SqlClient(self)
        self._stats_service = StatsClient(self)
        self._table_service = TableClient(self)
        self._udf_service = UDFClient(self)
        self._version_service = VersionClient(self)
        self._workbook_service = WorkbookClient(self)

        self._multiple_expserver = False
        self._expserver_ports = []
        self._socket_send_lock = threading.Lock()
        self._socket_recv_lock = threading.Lock()
        self._recv_in_progress = False
        self._pending_requests = {}
        self._finished_requests = {}

        try:
            expserver_count = int(os.environ['XCE_EXPSERVER_COUNT'])
            expserver_ports = os.environ['XCE_EXPSERVER_PORTS']
            if (expserver_count > 1):
                self._multiple_expserver = True
                self._expserver_ports = expserver_ports.split(',')
        except KeyError:
            pass

        self._sys_wkbk_name = sys_wkbk_name + self.username
        self._sys_session = None

    def _get_xce_socket(self):
        pid = os.getpid()
        if (not pid in self.xce_sockets):
            logger.debug(
                f'client.py: creating new socket for {pid} {str(self.xce_sockets)}'
            )
            xce_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            if xpu_host.is_xpu():
                xce_socket_path = xpu_host.get_socket_path()
            else:
                xce_socket_path = "/tmp/xcalar_sock/usrnode"
            xce_socket.connect(xce_socket_path)
            self.xce_sockets[pid] = xce_socket
        return self.xce_sockets[pid]

    # should never be exposed to the user
    def _execute(self, workItem):
        assert (self._legacy_xcalar_api is not None)
        return self._legacy_xcalar_api.execute(workItem)

    @property
    def username(self):
        return self._user_name

    def _send_buffer(self, buf):
        # Send over an 8 byte unsigned integer denoting payload size.
        msg_size = struct.pack("Q", len(buf))
        self._socket_send_lock.acquire()
        try:
            #
            # Send size and then payload. sendall raises an exception if something fails in sending
            #
            self._get_xce_socket().sendall(msg_size)
            self._get_xce_socket().sendall(buf)
        finally:
            self._socket_send_lock.release()

    #
    # Receive a reply to message with request_id.  Lots of threads could be doing the
    # receives at the same time so this code needs to sort out the replies and make
    # the caller gets the right one and all other receivers get the right replies as
    # well.
    #
    def _recv_buffer(self, our_request_id):
        self._socket_recv_lock.acquire()
        while (True):
            if (our_request_id in self._finished_requests):
                #
                # Our reply came before we got here so we are done
                #
                response_msg = self._finished_requests[
                    our_request_id].response_msg
                del self._finished_requests[our_request_id]
                self._socket_recv_lock.release()
                return response_msg
            elif (not self._recv_in_progress):
                #
                # Nobody is receiving so we need to.
                #
                if (our_request_id in self._pending_requests):
                    del self._pending_requests[our_request_id]
                break
            else:
                if (our_request_id not in self._pending_requests):
                    #
                    # Put our request_id in the _pending_requests dictionary so the receiver
                    # will know where to put our reply when it gets it.
                    #
                    self._pending_requests[our_request_id] = PendingRequest(
                        self._socket_recv_lock)

                self._pending_requests[our_request_id].wait()

        #
        # No thread is receiving data on the socket it is up to us to wait for the reply and
        # sort out the replies.
        #
        assert (not self._recv_in_progress)
        self._recv_in_progress = True

        while (True):
            self._socket_recv_lock.release()
            size_buffer = _recvall(self._get_xce_socket(), 8)
            response_size = struct.unpack("Q", size_buffer)[0]
            response_buffer = _recvall(self._get_xce_socket(), response_size)
            response_msg = ProtoMsg()
            response_msg.ParseFromString(response_buffer)
            response_request_id = response_msg.response.requestId
            self._socket_recv_lock.acquire()
            if (response_request_id == our_request_id):
                #
                # The message matches our id so we can return it. However,
                # before we do that we have to wake another thread that is
                # waiting for a response so it can do the work to receive it.
                #
                for pending_request in self._pending_requests.values():
                    pending_request.notify()
                    break
                self._recv_in_progress = False
                self._socket_recv_lock.release()
                return response_msg
            elif (response_request_id in self._pending_requests):
                #
                # The message is for another thread that is waiting for a response.
                # Save the response and wake up the thread.
                #
                pending_request = self._pending_requests[response_request_id]
                pending_request.response_msg = response_msg
                self._finished_requests[response_request_id] = pending_request
                del self._pending_requests[response_request_id]
                pending_request.notify()
            else:
                #
                # The message is for a thread that hasn't waited yet.
                # Save the response.
                #
                self._finished_requests[response_request_id] = PendingRequest(
                    self._socket_recv_lock, response_msg)

    # Send given message and wait for/receive response.
    def _send_msg(self, msg, request_id=None, serialize_msg=True):
        if serialize_msg:
            assert (request_id is None)
            request_id = msg.request.requestId
            msg = msg.SerializeToString()
        else:
            assert (request_id is not None)
        # send serialized message across the socket
        self._send_buffer(msg)
        return self._recv_buffer(request_id)

    def _serializeRequest(self, serviceReq):
        msg = ProtoMsg()
        msg.type = ProtoMsgTypeRequest

        self.next_request_id_lock.acquire()
        request_id = self.next_request_id
        msg.request.requestId = request_id
        self.next_request_id += 1
        self.next_request_id_lock.release()

        msg.request.childId = 0
        msg.request.target = ProtoMsgTargetService

        msg.request.servic.CopyFrom(serviceReq)

        reqBytes = msg.SerializeToString()

        return reqBytes, request_id

    def xceRequest(self, serviceName, methodName, request):
        serviceRequest = ServiceRequest()
        serviceRequest.serviceName = serviceName
        serviceRequest.methodName = methodName
        serviceRequest.body.Pack(request)

        reqBytes, request_id = self._serializeRequest(serviceRequest)

        bypass_exp_server = False
        # If we are an XPU or trying to bypass the proxy (if running on same machine as usrnode),
        # we can connect directly to the Usrnode Unix-domain socket and send
        # the request there. We do not need to go through expServer, which may be expensive.
        # However, there are some service methods that are actually implemented in expServer,
        # so for these we cannot bypass expServer. For the rest of the services, we will
        # connect directly to usrnode.
        if xpu_host.is_xpu() or self._bypass_proxy:
            # XXX This a hack. We need a better way to know which methods are
            # implemented in expServer.
            exp_server_needed = {
                "Dataflow": [
                    "Synthesize", "Sort", "Index", "GroupBy", "Join",
                    "UnionOp", "Project", "GenRowNum", "Map", "Aggregate",
                    "Filter", "Index"
                ],
                "Workbook": ["ConvertKvsToQuery"],
                "Sql": ["ExecuteSQL"]
            }

            if serviceName not in exp_server_needed.keys():
                bypass_exp_server = True
            else:
                if methodName not in exp_server_needed[serviceName]:
                    bypass_exp_server = True

        if bypass_exp_server:
            msg = self._send_msg(
                reqBytes, request_id=request_id, serialize_msg=False)
        else:
            jsonData = {"data": base64.b64encode(reqBytes).decode("ascii")}
            xce_url = self._url
            if (self._multiple_expserver):
                parsed_xce_url = urlparse(XcalarServiceXceUrl)
                xce_port_idx = random.randint(0,
                                              len(self._expserver_ports) - 1)
                newUrlHost = "{}:{}".format(
                    parsed_xce_url.hostname,
                    self._expserver_ports[xce_port_idx])
                parsed_xce_url = parsed_xce_url._replace(netloc=newUrlHost)
                xce_url = urlunparse(parsed_xce_url)

            resp = self._session.post(
                xce_url, json=jsonData, verify=self.auth.verifySsl)

            respBytes = None
            # valid response is a json with "data" or "error" attributes
            try:
                json_resp = resp.json()
                if "error" in json_resp:
                    resp.reason = json_resp["error"]
                resp.raise_for_status()
                respBytes = base64.b64decode(json_resp["data"])
            except (ValueError, KeyError) as ex:
                # includes JSONDecodeError,
                # raise httpError if exists, else raise json error
                resp.raise_for_status()
                raise RuntimeError("Invalid response") from ex

            msg = ProtoMsg()
            msg.ParseFromString(respBytes)

        # XXX our status codes should probably be enum-ified
        if msg.response.status != 0:
            raise XDPException(msg.response.status, msg.response.error)

        serviceResponse = msg.response.servic
        unpackedResponse = serviceResponse.body

        return unpackedResponse

    def global_kvstore(self):
        return xcalar.external.kvstore.KvStore.global_(self)

    def runtime(self):
        return xcalar.external.runtime.Runtime(self)

    def get_num_nodes_in_cluster(self):
        """
        This method returns the number of nodes in the cluster.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> num_nodes = client.get_num_nodes_in_cluster()

        :returns: The number of nodes in cluster.
        :return_type: :obj:`int`

        """
        getNumNodesWorkItem = WorkItemGetNumNodes()
        numNodes = self._execute(getNumNodesWorkItem)

        return numNodes.numNodes

    def get_libstats(self):
        """ Pulls libstats for this cluster node (the cluster node this client object is connected to)"""
        req = GetLibstatsRequest()
        res = self._stats_service.getLibstats(req)
        return res

    def get_metrics_from_node(self, node_id):
        """
        This method returns Runtime Metrics of a single specified node
        in the cluster.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> node_metrics = client.get_metrics_from_node(0)

        :param node_id: *Required*. Node in cluster from which metrics will
                        be retrieved.
        :type node_id: int
        :Returns:
            A list of dicts, where each dict contains relevant information
            about that Metric.
            (:ref:`metrics_list_ref`)
        :return_type: list

        * :Response Structure:
            * (*list*)
                * (*dict*)
                    * **metric_name** (*str*) - The name of the metric being\
                                                considered
                    * **metric_value** (*int*) - The numerical value of the\
                                                 metric being considered
                    * **metric_type** (*int*) - The type of the metric
                    * **group_id** (*int*) - The group under which\
                                             the metric falls
                    * **group_name** (*str*) - Name of group under which this\
                                               metric falls

        :raises TypeError: if node_id is not an int
        :raises ValueError:
            if node_id is less than 0 or
            greater than (number_of_nodes_in_cluster-1)

        """
        # need this because True/False is instance of int
        if (isinstance(node_id, bool)):
            raise TypeError("node_id must be int, not '{}'".format(
                type(node_id)))

        if (not isinstance(node_id, int)):
            raise TypeError("node_id must be int, not '{}'".format(
                type(node_id)))

        num_nodes = self.get_num_nodes_in_cluster()

        if (node_id >= num_nodes or node_id < 0):
            raise ValueError("node_id must be a valid node in the cluster "
                             "(0 <= node_id < num_nodes=%d)" % num_nodes)

        getStatsWorkItem = WorkItemGetStats(nodeId=node_id)
        metrics = self._execute(getStatsWorkItem)

        map_group_id_name = self._get_group_id_map(node_id)

        metrics_list = []
        for stat in metrics:

            group_id = stat.groupId
            if (group_id in map_group_id_name):
                group_name = map_group_id_name[group_id][0]
            else:
                group_name = ""

            metric_info = {
                "metric_name": stat.statName,
                "metric_value": stat.statValue,
                "metric_type": stat.statType,
                "group_id": group_id,
                "group_name": group_name
            }
            metrics_list.append(metric_info)
        return metrics_list

    def reset_libstats(self, node_id=None, cumulative=True, hwm=True):
        """
        By default, this method resets cumulative and HWM runtime stats in all cluster nodes.
        If desired you can specify individual nodes or to only reset either cumulative or
        HWM stats (instead of both, which is the default).

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> client.reset_libstats()

        :param node_id:
            *Optional*. Node in cluster for which metric will be reset. By default, all cluster
            nodes will be reset.
        :type node_id: int
        :raises TypeError: if node_id is not an int
        :raises ValueError:
            if node_id is less than 0 or
            greater than (number_of_nodes_in_cluster-1)

        :param cumulative:
            Default is True. Set to False if you want to only reset hwm stats.
        :type cumulative: bool
        :raises TypeError: if cumulative is not an bool

        :param hwm:
            Default is True. Set to False if you want to only reset cumulative stats.
        :type hwm: bool
        :raises TypeError: if hwm is not an bool
        """
        if node_id is not None:
            # need this because True/False is instance of int
            if (isinstance(node_id, bool)):
                raise TypeError("node_id must be int, not '{}'".format(
                    type(node_id)))
            if (not isinstance(node_id, int)):
                raise TypeError("node_id must be int, not '{}'".format(
                    type(node_id)))
            num_nodes = self.get_num_nodes_in_cluster()

            if (node_id >= num_nodes or node_id < 0):
                raise ValueError("node_id must be a valid node in the cluster "
                                 "(0 <= node_id < num_nodes=%d)" % num_nodes)

        if (not isinstance(cumulative, bool)):
            raise TypeError("cumulative must be bool, not '{}'".format(
                type(cumulative)))

        if (not isinstance(hwm, int)):
            raise TypeError("hwm must be bool, not '{}'".format(type(hwm)))

        req = ResetStatsRequest()
        if node_id is not None:
            req.nodeId = node_id
        else:
            # A nodeId of -1 indicates all nodes
            req.nodeId = -1
        req.resetCumulativeStats = cumulative
        req.resetHwmStats = hwm
        self._stats_service.resetStats(req)

    # This method should never be exposed to the user because
    # group_id->group_name mappings are exposed to the user through
    # get_metrics_from_node
    def _get_group_id_map(self, node_id):
        assert (node_id is not None)

        node_id = int(node_id)
        assert (node_id >= 0)

        getGroupIdMapWorkItem = WorkItemGetStatGroupIdMap(nodeId=node_id)
        result = self._execute(getGroupIdMapWorkItem)

        map_id_name = {}
        for group in result.groupNameInfoArray:
            group_id = group.groupIdNum
            group_name = group.statsGroupName
            group_stats = group.totalSingleStats

            if (group_id not in map_id_name):
                map_id_name[group_id] = (group_name, group_stats)

        return map_id_name

    def get_version(self):
        """
        This method returns current Xcalar Design version information.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> version = client.get_version()

        :returns: The version of Xcalar Design installed.

        """
        return self._version_service.getVersion(Empty())

    def get_config_params(self, param_names=None):
        """
        This method returns the current configuration parameters.
        If param_names are given as input,
        returns only params associated to them.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> config_params = client.get_config_params()

        :param param_names: *Optional*. Names of parameters you want to get.
                    Refer to :ref:`config_list_ref` to get paramater names
                    (plus additional information).
                    If None, returns all parameters.
        :Returns:
            A list of dicts, where each dict contains relavent information
            about that Parameter.
            (:ref:`config_list_ref`)
        :return_type: list

        * :Response Structure:
            * (*list*)
                * (*dict*)
                    * **param_name** (*str*) - The name of the Parameter
                    * **param_value** (*str*) - The value of the Parameter
                    * **visable** (*bool*) - Whether the parameter is made \
                                             visible to all users in \
                                             Xcalar Design
                    * **changeable** (*bool*) - Whether the parameter \
                                                can be changed
                    * **restart_required** (*bool*) - Whether a restart is \
                                                      required for a parameter\
                                                      change to have effect
                    * **default_value** (*str*) - Default value of the \
                                                  parameter

        """
        workItem = WorkItemGetConfigParams()
        result = self._execute(workItem)

        parameters = result.parameter
        params = []
        for param in parameters:
            param_info = {
                "param_name": param.paramName,
                "param_value": param.paramValue,
                "visible": param.visible,
                "changeable": param.changeable,
                "restart_required": param.restartRequired,
                "default_value": param.defaultValue
            }
            params.append(param_info)
        self._config_params = params
        if not param_names:
            return params
        result_params = []
        given_param_name_set = set(param_names)
        for param in self._config_params:
            if param['param_name'] in given_param_name_set:
                result_params.append(param)
                given_param_name_set.remove(param['param_name'])
        if given_param_name_set:
            raise ValueError(
                f"No such param name(s): {','.join(given_param_name_set)}")
        return result_params

    def set_config_param(self,
                         param_name,
                         param_value=None,
                         set_to_default=False):
        """
        This method sets a specified parameter to the desired value.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> client.set_config_param("SwapUsagePercent", 90, False)

        :param param_name: *Required*. Name of parameter you want to set.
                    Refer to :ref:`config_list_ref` to get paramater names
                    (plus additional information).
        :param param_value: *May be Required*. Value you want to set the
                    parameter to. If *set_to_default*
                    is set to True, this value will be ignored.
        :param set_to_default: *Optional*. If set to True, parameter will be
                                           set to its default value.
        :type param_name: str
        :type param_value: str or int or bool
        :type set_to_default: bool
        :raises TypeError: if invalid parameter types
        :raises ValueError: param_name is not valid/does not exist
        """
        if (not isinstance(set_to_default, bool)):
            raise TypeError("set_to_default must be bool, not '{}'".format(
                type(set_to_default)))
        if (not isinstance(param_name, str)):
            raise TypeError("param_name must be str, not '{}'".format(
                type(param_name)))
        if ((not set_to_default) and (not isinstance(param_value,
                                                     (int, str, bool)))):
            raise TypeError(
                "param_value must be str, bool or int, not '{}'".format(
                    type(param_value)))

        param_found = False
        default_value = None

        if (self._config_params is not None):
            params = self._config_params
        else:
            params = self.get_config_params()

        for param in params:
            if (param["param_name"] == param_name):
                param_found = True
                default_value = param["default_value"]
                break

        if (not param_found):
            raise ValueError("No such param_name: '{}'".format(param_name))

        if (set_to_default):
            param_value = default_value

        param_value = str(param_value)

        workItem = WorkItemSetConfigParam(param_name, param_value)
        self._execute(workItem)

    def _create_system_workbook(self):
        # XXX system workbook per user: SDK-826
        num_retries = 2
        for retry in range(num_retries):
            try:
                sessions = self._list_sessions(self._sys_wkbk_name)
                if len(sessions) == 0:
                    workItem = WorkItemSessionNew(
                        name=self._sys_wkbk_name,
                        fork=False,
                        userName=self.username,
                        userIdUnique=self.user_id)
                    res = self._execute(workItem)
                    session_id = res.output.outputResult.sessionNewOutput.sessionId
                else:
                    session_id = sessions[0].sessionId
                wb = xcalar.external.workbook.Workbook(
                    client=self,
                    workbook_name=self._sys_wkbk_name,
                    workbook_id=session_id)
                self._sys_session = wb.activate()
                return
            except LegacyXcalarApiStatusException as e:
                if e.status != StatusT.StatusSessionExists or retry == num_retries - 1:
                    raise e

    def create_workbook(self, workbook_name, active=True):
        """
        This method creates a workbook and returns a
        :class:`Workbook <xcalar.external.workbook.Workbook>`
        instance representing this
        newly created workbook.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.create_workbook(workbook_name="create_demo")

        :param workbook_name: *Required*. Unique name that identifies the new
                                          workbook.
        :param active: *Optional*. Indicates whether or not the workbook will
                                   be active. Default value is True.
        :type workbook_name: str
        :type active: bool
        :Returns: A :class:`Workbook <xcalar.external.workbook.Workbook>`
                instance, which represents the newly created workbook.
        :return_type: :class:`Workbook <xcalar.external.workbook.Workbook>`
        :raises TypeError: Incorrect parameter types
        :raises XcalarApiStatusException:
            if workbook already exists or cluster out of resources.
        """
        if (not isinstance(workbook_name, str)):
            raise TypeError("workbook_name must be str, not '{}'".format(
                type(workbook_name)))

        if (not isinstance(active, bool)):
            raise TypeError("active must be bool, not '{}'".format(
                type(active)))

        # XXX system workbook per user: SDK-826
        if workbook_name.startswith(sys_wkbk_name):
            raise ValueError(
                f"Invalid workbook_name, '{workbook_name}', "
                f"workbook_name can't start with '{sys_wkbk_name}'")

        workItem = WorkItemSessionNew(
            name=workbook_name,
            fork=False,
            userName=self.username,
            userIdUnique=self.user_id)
        result = self._execute(workItem)
        workbook_id = result.output.outputResult.sessionNewOutput.sessionId
        workbook_obj = xcalar.external.workbook.Workbook(
            client=self, workbook_name=workbook_name, workbook_id=workbook_id)
        if (active):
            workbook_obj.activate()

        return workbook_obj

    def upload_workbook_from_target(self, workbook_name, connector_name,
                                    workbook_path):
        """
        This method uploads a workbook by creating a workbook with the supplied
        name and initializing it with the supplied content from a file present on
        target at given path. The method returns
        a :class:`Workbook <xcalar.external.workbook.Workbook>` instance
        representing this newly uploaded workbook.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.create_workbook(
                    workbook_name="download_workbook")
            >>> workbook.download_to_connector("Default Shared Root", "/tmp/wb1.tar.gz")
            >>> uploaded_workbook = client.upload_workbook_from_target(
                    "upload_demo", "Default Shared Root", "/tmp/wb1.tar.gz")

        :param workbook_name: *Required.* Name of the workbook to be uploaded
        :type workbook_name: str
        :param connector_name: *Required.* Connector name to use to read
                            workbook contents.
        :type connector_name: str
        :param workbook_path: *Required.* path to workbook tar.gz file to read
                            using connector.
        :type workbook_path: str

        :Returns: A :class:`Workbook <xcalar.external.workbook.Workbook>`
            instance, which represents the newly uploaded workbook.
        :return_type: :class:`Workbook <xcalar.external.workbook.Workbook>`
        :raises TypeError: if workbook_name is not a str.
        :raises RuntimeError:
            if not able to get workbook contents from target or for any other errors.
        """
        if (not isinstance(workbook_name, str)):
            raise TypeError("workbook_name must be str, not '{}'".format(
                type(workbook_name)))
        app_mgr = App(self)
        app_input = {
            "op": "upload_from_target",
            "notebook_name": workbook_name,
            "connector_name": connector_name,
            "notebook_path": workbook_path,
            "user_name": self.username
        }
        response = app_mgr.run_py_app(app_mgr.NotebookAppName, False,
                                      json.dumps(app_input))
        result = json.loads(response[0])[0][0]
        error = json.loads(response[1])[0][0]
        if error:
            raise RuntimeError(error)
        workbook_id = json.loads(result)['session_id']
        return xcalar.external.workbook.Workbook(
            client=self, workbook_name=workbook_name, workbook_id=workbook_id)

    def upload_workbook(self, workbook_name, workbook_content):
        """
        This method uploads a workbook by creating a workbook with the supplied
        name and initializing it with the supplied content. The method returns
        a :class:`Workbook <xcalar.external.workbook.Workbook>` instance
        representing this newly uploaded workbook.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.create_workbook(
                    workbook_name="download_workbook")
            >>> dl = workbook.download()
            >>> uploaded_workbook = client.upload_workbook(
                    "upload_demo", workbook_content = dl)

        :param workbook_name: *Required.* Name of the workbook to be uploaded
        :type workbook_name: str
        :param workbook_content: *Required.* A byte stream of the workbook
                        file, in the
                        Consolidated Unix Archive format (aka TAR format).
                        Such files have the ".tar.gz" file extension on
                        UNIX/LINUX systems. This is typically obtained from a
                        prior \
         :meth:`Workbook.download <xcalar.external.workbook.Workbook.download>`
                        invocation.
        :Returns: A :class:`Workbook <xcalar.external.workbook.Workbook>`
            instance, which represents the newly uploaded workbook.
        :return_type: :class:`Workbook <xcalar.external.workbook.Workbook>`
        :raises TypeError: if workbook_name is not a str.
        :raises XcalarApiStatusException:
            if workbook already exists or cluster out of resources.
        """
        if (not isinstance(workbook_name, str)):
            raise TypeError("workbook_name must be str, not '{}'".format(
                type(workbook_name)))

        # XXX This still needs some more thought. This assumes that the path to
        # addtional files will always be in a specific format (a format that
        # XD has decided on). However, if XD decides to change the additional
        # file directory layout, this would break the API since the
        # path_to_addtional_files would point to some nonexistant directory.
        path_to_additional_files = "jupyterNotebooks/{}-{}/".format(
            self.username, workbook_name)

        workItem = WorkItemSessionUpload(
            name=workbook_name,
            sessionContent=workbook_content,
            pathToAdditionalFiles=path_to_additional_files,
            userName=self.username,
            userIdUnique=self.user_id)

        result = self._execute(workItem)
        return xcalar.external.workbook.Workbook(
            client=self,
            workbook_name=workbook_name,
            workbook_id=result.sessionId)

    def get_workbook(self, workbook_name):
        """
        This method returns a
        :class:`Workbook <xcalar.external.workbook.Workbook>` instance
        representing an existing workbook (with the specified name),
        with which useful tasks can be performed.

        .. testsetup::

            client.create_workbook("get_demo")

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbook = client.get_workbook(workbook_name = "get_demo")

        :param workbook_name: *Required.* Name of workbook.
        :type pattern: str
        :Returns: A :class:`Workbook <xcalar.external.workbook.Workbook>`
                instance which represents the workbook
                (with the specified name).
        :return_type: :class:`Workbook <xcalar.external.workbook.Workbook>`
        :raises TypeError: if workbook_name is not a str
        :raises ValueError: if workbook does not exist,
                or multiple workbooks found matching workbook_name
        """
        if (not isinstance(workbook_name, str)):
            raise TypeError("workbook_name must be str, not '{}'".format(
                type(workbook_name)))

        workbooks = self.list_workbooks(workbook_name)
        if (len(workbooks) == 0):
            raise ValueError("No such workbook: '{}'".format(workbook_name))

        if (len(workbooks) > 1):
            raise ValueError("Multiple workbooks found matching: '{}'".format(
                workbook_name))

        return workbooks[0]

    def list_workbooks(self, pattern="*"):
        """
        This method returns a list of Workbooks matching the search pattern.
        Each element of the list is an instance of
        ':class:`Workbook <xcalar.external.workbook.Workbook>`.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbooks = client.list_workbooks()

        :param pattern: *Optional.* Search pattern. Will only list workbooks
                        that match the search pattern. Default is "*", which
                        lists all workbooks.
        :type pattern: str

        :return_type:
            list[:class:`Workbook <xcalar.external.workbook.Workbook>`]

        :raises TypeError: if pattern is not a str
        """
        if (not isinstance(pattern, str)):
            raise TypeError("pattern must be str, not '{}'".format(
                type(pattern)))

        sessions = self._list_sessions(pattern)
        workbooks = []
        for session in sessions:
            # hide the system workbook: SDK-826
            if session.name.startswith(sys_wkbk_name):
                continue
            workbook_obj = xcalar.external.workbook.Workbook(
                client=self,
                workbook_name=session.name,
                workbook_id=session.sessionId,
                info=session.info,
                active_node=session.activeNode)
            workbooks.append(workbook_obj)

        return workbooks

    # Dataflows are essentially workbooks/sessions containing a single dataflow
    # saved as the value for the dataflow_optimized_execution_key.  Any Udf modules
    # needed by the dataflow are also in the session.  All of these workbooks
    # are owned by a special user ".xcalar.published.df" (same user that XD
    # uses).

    def destroy_workbook(self, workbook_name):
        """
        This method destroys the specified workbook.
        """
        raise NotImplementedError()

    def list_global_tables(self, pattern="*"):
        """
        This method lists all global tables of user available in the system.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> tables = client.list_global_tables()
        """
        if not isinstance(pattern, str):
            raise TypeError("pattern must be str, not '{}'".format(
                type(pattern)))
        # XXX system workbook per user
        if self._sys_session is None:
            self._create_system_workbook()

        list_req = ListTablesRequest()
        list_req.pattern = pattern
        scope = WorkbookScope()
        scope.globl.SetInParent()
        list_req.scope.CopyFrom(scope)
        list_res = self._table_service.listTables(list_req)

        tables = [
            xcalar.external.table.Table._table_from_proto_response(
                self._sys_session, tab_name, tab_meta)
            for tab_name, tab_meta in list_res.table_meta_map.items()
        ]
        return tables

    def list_tables(self, pattern="*", session_name_pattern="*"):
        """
        This method lists all tables across all sessions of user available in the system.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> tables = client.list_tables()
        """
        if not isinstance(pattern, str):
            raise TypeError("pattern must be str, not '{}'".format(
                type(pattern)))
        sessions = self.list_sessions(pattern=session_name_pattern)
        tables = []
        for sess in sessions:
            try:
                tabs = sess.list_tables(pattern=pattern)
                tables.extend(tabs)
            except XDPException as ex:
                # session could have deactivated or
                # deleted after listing it, so ignore
                if ex.statusCode not in [
                        StatusT.StatusSessionNotFound,
                        StatusT.StatusSessionInact
                ]:
                    raise
        return tables

    def get_global_table(self, table_name):
        """
        This method returns the global table matching the specified name.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> table = client.get_table("session_demo")

        :param table_name: *Required.* Unique name that identifies the table to
            get
        :type table_name: str
        :raises TypeError: Incorrect parameter type
        """
        if not isinstance(table_name, str):
            raise TypeError("table_name must be str, not '{}'".format(
                type(table_name)))

        tables = self.list_global_tables(table_name)
        if len(tables) == 0:
            raise ValueError("No such table: '{}'".format(table_name))

        if len(tables) > 1:
            raise ValueError(
                "Multiple tables found matching: '{}'".format(table_name))

        return tables[0]

    def add_dataflow(self, dataflow_name, dataflow_contents):
        """
        This method adds a dataflow to the cluster by creating a
        workbook/session with the supplied name and initializing it with the
        supplied content.  The method returns a
        :class:`Workbook <xcalar.external.workbook.Workbook>` instance
        representing this newly added dataflow.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> with open(dataflowFilePath, 'rb') as dfFile:
            >>>     dfContent = dfFile.read()
            >>> dataflow = client.add_dataflow(dataflow_name="upload_demo",
            >>>                                dataflow_contents=dfContent)

        :param dataflow_name: *Required.* Name for the dataflow being added
        :type dataflow_name: str
        :param dataflow_contents: *Required.* A byte stream of the dataflow
                        file, in the Consolidated Unix Archive format (aka TAR
                        format). Such files have the ".tar.gz" file extension on
                        UNIX/LINUX systems.
        :Returns: A :class:`Workbook <xcalar.external.workbook.Workbook>`
            instance, which represents the newly uploaded dataflow.
        :return_type: :class:`Workbook <xcalar.external.workbook.Workbook>`
        :raises TypeError: if dataflow_name is not a str.
        :raises XcalarApiStatusException:
            if dataflow already exists or cluster out of resources.
        """

        if (not isinstance(dataflow_name, str)):
            raise TypeError("dataflow_name must be str, not '{}'".format(
                type(dataflow_name)))

        # Upload the dataflow into the session as a special session belonging
        # to dataflow_user_name.
        workItem = WorkItemSessionUpload(
            name=dataflow_name,
            sessionContent=dataflow_contents,
            pathToAdditionalFiles=None,
            userName=dataflow_user_name,
            userIdUnique=dataflow_user_id)
        result = self._execute(workItem)

        workbook_obj = xcalar.external.workbook.Workbook(
            client=self,
            workbook_name=dataflow_name,
            workbook_id=result.sessionId,
            workbook_user=dataflow_user_name)
        workbook_obj.activate()

        return workbook_obj

    def delete_dataflow(self, dataflow_name):
        """
        This method deletes a dataflow from the cluster.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> client.delete_dataflow("my_dataflow")

        :parm dataflow_name: *Required.* Name of the dataflow to delete
        :type dataflow_name: str
        :Returns: XcalarApiSessionGenericOutput format
        :raises TypeError: if dataflow_name is not a str.
        """
        if (not isinstance(dataflow_name, str)):
            raise TypeError("dataflow_name must be str, not '{}'".format(
                type(dataflow_name)))

        # Set the session to None otherwise execute() (in XcalarApi.py)
        # changes the username to that of the session and we want the
        # special username used.
        self._legacy_xcalar_api.setSession(None)

        df_workbook = self.get_dataflow(dataflow_name)
        df_workbook.inactivate()

        workItem = WorkItemSessionDelete(
            namePattern=dataflow_name,
            userName=dataflow_user_name,
            userIdUnique=dataflow_user_id)
        self._execute(workItem)

    def list_dataflows(self, name_pattern="*"):
        """
        This method returns a list of the dataflows matching the pattern.
        Each element of the list is an instance of
        ':class:`Workbook <xcalar.external.workbook.Workbook>`.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> dataflows = client.list_dataflows()

        :param name_pattern: *Optional.* Search pattern. Will only list
                             dataflows that match the search pattern.  Default
                             is "*", which lists all dataflows.
        :type name_pattern: str

        :return_type:
            list[:class:`Workbook <xcalar.external.workbook.Workbook>`]

        :raises TypeError: if pattern is not a str
        """
        workItem = WorkItemSessionList(
            pattern=name_pattern,
            userName=dataflow_user_name,
            userIdUnique=dataflow_user_id)
        # if no dataflows found, returns empty list instead of error
        try:
            sessions = self._execute(workItem).sessions
        except LegacyXcalarApiStatusException as e:
            if e.message == "Session does not exist":
                return []
            raise e

        dataflows = []
        for session in sessions:
            workbook_obj = xcalar.external.workbook.Workbook(
                client=self,
                workbook_name=session.name,
                workbook_id=session.sessionId,
                workbook_user=dataflow_user_name,
                info=session.info,
                active_node=session.activeNode)
            dataflows.append(workbook_obj)

        return dataflows

    def get_dataflow(self, dataflow_name):
        """
        This method returns a
        :class:`Workbook <xcalar.external.workbook.Workbook>` instance
        representing an existing dataflow (with the specified name),
        with which useful tasks can be performed.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> dataflow = client.get_dataflow(dataflow_name = "get_demo")

        :param dataflow_name: *Required.* Name of dataflow.
        :type pattern: str
        :Returns: A :class:`Workbook <xcalar.external.workbook.Workbook>`
                instance which represents the dataflow
                (with the specified name).
        :return_type: :class:`Workbook <xcalar.external.workbook.Workbook>`
        :raises TypeError: if dataflow_name is not a str
        :raises ValueError: if dataflow does not exist,
                or multiple dataflows found matching dataflow_name
        """
        if (not isinstance(dataflow_name, str)):
            raise TypeError("dataflow_name must be str, not '{}'".format(
                type(dataflow_name)))

        dataflows = self.list_dataflows(dataflow_name)
        if (len(dataflows) == 0):
            raise ValueError("No such dataflow: '{}'".format(dataflow_name))

        if (len(dataflows) > 1):
            raise ValueError("Multiple dataflows found matching: '{}'".format(
                dataflow_name))

        return dataflows[0]

    def execute_dataflow(self,
                         dataflow_name,
                         inside_session,
                         params,
                         newTableName=None,
                         queryName=None,
                         parallel_operations=False,
                         clean_job_state=True,
                         pin_results=False,
                         sched_name=None):
        """
        This method "executes" a dataflow by:
            * Running the specified dataflow (which must have been previously
              added via add_dataflow() inside the specified session
              (inside_session).

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> session = SessionNew
            >>> results = client.execute_dataflow("execute_demo", session)

        :param inside_session: *Required.* Name of the session in which the
                        dataflow is run.  Any resultant table will reside in
                        this session.
        :param dataflow_name: *Required.* Name of the dataflow to be executed
        :type dataflow_name: str
        :param params: parameters.  A set of <key>=<value> parameters where all
                        instances of "key" (note the angle brackets are not
                        specified) are replaced with "value".
        :param newTableName: name of resultant table (execute to table)
        :param queryName: name of query
        """

        # Set the session to None otherwise execute() (in XcalarApi.py)
        # changes the username to that of the session and we want the
        # special username used.
        self._legacy_xcalar_api.setSession(None)

        df_workbook = self.get_dataflow(dataflow_name)

        df_workbook.activate()
        self._legacy_xcalar_api.setSession(df_workbook)
        # Now get query from KvStore
        dfKvstore = KvStore.workbook_by_name(self, df_workbook.username,
                                             df_workbook.name)
        queryStr = dfKvstore.lookup(dataflow_optimized_execution_key)

        # Create a json object from the query
        inobj = json.loads(queryStr)

        # Extract the interesting field from the query
        retina = inobj['retina']

        if not queryName:
            queryName = dataflow_name

        # Do param substitution in the 'retina'.  Note this is purely a string
        # substitution.  Changing "<param1>" in the query would specify the
        # key as "param1" and the substituted value.
        retinaWParams = retina
        if params:
            for key in params.keys():
                replacementKey = "<{}>".format(key)
                retinaWParams = re.sub(replacementKey, params[key],
                                       retinaWParams)

        # add the json query as a retina
        self._retina.add(
            queryName,
            retinaJsonStr=retinaWParams,
            udfUserName=dataflow_user_name,
            udfSessionName=dataflow_name)

        # Set the session to the one specified by the caller.  This is the
        # session where the execution of the dataflow should occur.  And any
        # resulant table is left.
        self._legacy_xcalar_api.setSession(inside_session)

        if newTableName:
            self._retina.execute(
                queryName, [],
                newTableName,
                udfUserName=dataflow_user_name,
                udfSessionName=dataflow_name,
                parallel_operations=parallel_operations,
                pin_results=pin_results,
                sched_name=sched_name,
                clean_job_state=clean_job_state)
            output = self._legacy_xcalar_api.listTable(newTableName)
            ret = output.nodeInfo[0].name
        else:
            ret = self._retina.execute(queryName, [])

        self._retina.delete(queryName)

        return ret

    def add_data_target(self, target_name, target_type_id, params={}):
        """
        This method adds a data target. The desired target type and its
        respective parameters need to be provided. Refer to
        :ref:`data_target_types_list_ref` to get a list of data target types,
        plus relevant information such as type_id and required/optional
        parameters for each data target type.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> params = {}
            >>> params['mountpoint'] = "/netstore"
            >>> data_target = client.add_data_target(target_name="target_demo",
            ...                                      target_type_id="shared",
            ...                                      params=params)

        :param target_name: *Required.* A unique name to identify the target
        :param target_type_id: *Required.* The type ID for the desired target
                type (:ref:`data_target_types_list_ref`)
        :param params: A dictionary, where the key is the parameter name and
                    value is the parameter value.  *May be required*, depending
                    on target_type. Refer to :ref:`data_target_types_list_ref`
                    to get a list of data target types and information about
                    their respective optional/required parameters.
        :type target_name: str
        :type target_type_id: str
        :type params: dict
        :raises TypeError: if target_name, target_type_id or params is
                    an invalid type
        :raises ValueError: if target_type_id does not exist, or if params
                            is invalid for the specified target_type

        :Returns: The created
            :class:`DataTarget <xcalar.external.data_target.DataTarget>`
        :return_type:
            :class:`DataTarget <xcalar.external.data_target.DataTarget>`
        """
        if (not isinstance(target_name, str)):
            raise TypeError("target_name must be str, not '{}'".format(
                type(target_name)))

        if (not isinstance(target_type_id, str)):
            raise TypeError("target_type_id must be str, not '{}'".format(
                type(target_type_id)))

        if (not isinstance(params, dict)):
            raise TypeError("params must be dict, not '{}'".format(
                type(params)))

        target_types = self._list_data_target_types()

        target_type_name = ""
        required_params = []
        optional_params = []
        for target_type in target_types:
            if (target_type['type_id'] == target_type_id):
                target_type_name = target_type['type_name']
                for param in target_type['parameters']:
                    if (param["optional"]):
                        optional_params.append(param["name"])
                    else:
                        required_params.append(param["name"])
                break

        if (target_type_name == ""):
            raise ValueError(
                "No such target_type_id: '{}'".format(target_type_id))

        unexpected_params = (
            set(params) - (set(required_params) | set(optional_params)))
        for param in unexpected_params:
            # XXX "listUdf" is used in test_parquet even though it is not in
            # the optional/required params
            if (param != "listUdf"):
                raise ValueError("No such parameter: '{}'".format(param))

        missing_params = set(required_params) - set(params)
        if (len(missing_params) != 0):
            raise ValueError(
                "The following required parameters are missing: '{}'".format(
                    str(list(missing_params))))

        workItem = WorkItemAddTarget2(
            targetTypeId=target_type_id,
            targetName=target_name,
            targetParams=params)

        self._execute(workItem)

        data_target_obj = DataTarget(
            self,
            target_name=target_name,
            target_type_id=target_type_id,
            target_type_name=target_type_name,
            params=params)
        return data_target_obj

    def list_data_targets(self):
        """
        This method returns the list of data targets in the system.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> data_targets = client.list_data_targets()

        :Returns: A list comprised of each
            :class:`DataTarget <xcalar.external.data_target.DataTarget>`
            currently in the system.
        :return_type:
            list[:class:`DataTarget <xcalar.external.data_target.DataTarget>`]

        """
        workItem = WorkItemListTargets2()
        data_targets = self._execute(workItem)

        ret_val = []
        for data_target in data_targets:
            data_target_obj = DataTarget(
                self,
                target_name=data_target["name"],
                target_type_id=data_target["type_id"],
                target_type_name=data_target["type_name"],
                params=data_target["params"])
            ret_val.append(data_target_obj)

        return ret_val

    def get_data_target(self, target_name):
        """
        This method returns a
        :class:`DataTarget <xcalar.external.data_target.DataTarget>` instance
        representing an existing data target with the specified name.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> data_target = client.get_data_target("target_demo")

        .. testcleanup::

            client.get_data_target("target_demo").delete()

        :param target_name: *Required.* Name of data target.
        :type target_name: str
        :raises TypeError: if target_name is not a str
        :Returns: The requested
            :class:`DataTarget <xcalar.external.data_target.DataTarget>` .
        :return_type:
            :class:`DataTarget <xcalar.external.data_target.DataTarget>`

        """
        if (not isinstance(target_name, str)):
            raise TypeError("target_name must be str, not '{}'".format(
                type(target_name)))

        targets = self.list_data_targets()

        for target in targets:
            if (target.name == target_name):
                return target

        raise ValueError("No such target_name: '{}'".format(target_name))

    # This does not need to be exposed to the user because they can get this
    # info from the documentation
    def _list_data_target_types(self):
        if (self._data_target_types is None):
            workItem = WorkItemListTargetTypes2()
            self._data_target_types = self._execute(workItem)
        return self._data_target_types

    def list_drivers(self):
        """
        This method returns a list of drivers
        Each element of the list is an instance of
        :class:`Driver <xcalar.external.driver.Driver>`, on which driver
        tasks can be performed.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> drivers = client.list_drivers()

        :Returns: A list of :class:`Driver <xcalar.external.driver.Driver>` .
        :return_type: list[:class:`Driver <xcalar.external.driver.Driver>`]

        """
        request = App_pb2.DriverRequest()
        request.input_json = json.dumps({
            "func": "listDrivers",
        })
        response = self._app_service.driver(request)
        driver_data_list = json.loads(response.output_json)

        drivers = [
            Driver(self, d["name"], d["description"], d["params"],
                   d["isBuiltin"]) for d in driver_data_list
        ]

        return drivers

    def get_driver(self, driver_name):
        """
        This method returns a
        :class:`Driver <xcalar.external.driver.Driver>` instance
        representing an existing driver (with the specified name), with which
        useful tasks can be performed.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> driver = client.get_driver("driver_demo")

        .. testcleanup::

            client.get_driver("driver_demo").delete()

        :param driver_name: *Required.* Name of data driver.
        :type driver_name: str
        :raises TypeError: if driver_name is not a str
        :Returns: A :class:`Driver <xcalar.external.driver.Driver>` instance,
                which represents the data driver (with the specified name).
        :return_type: :class:`Driver <xcalar.external.driver.Driver>`

        """
        if (not isinstance(driver_name, str)):
            raise TypeError("driver_name must be str, not '{}'".format(
                type(driver_name)))

        for driver in self.list_drivers():
            if (driver.name == driver_name):
                return driver

        raise ValueError("No such driver_name: '{}'".format(driver_name))

    def list_datasets(self, pattern="*"):
        """
        This method returns a list of Datasets matching the search pattern.
        Each element of the list is an instance of
        :class:`Dataset <xcalar.external.dataset.Dataset>`, on which Dataset
        tasks can be performed.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> workbooks = client.list_datasets()

        :param pattern: *Optional.* Search pattern to match against all
            datasets. Default is "*", which matches all datasets.
        :type pattern: str

        :Returns: A list of :class:`Dataset <xcalar.external.dataset.Dataset>`
            instances, where each instance represents that dataset.
        :return_type: list[:class:`Dataset <xcalar.external.dataset.Dataset>`]

        :raises TypeError: if pattern is not a str
        """
        if (not isinstance(pattern, str)):
            raise TypeError("pattern must be str, not '{}'".format(
                type(pattern)))

        workItem = WorkItemGetDatasetsInfo(pattern)

        # if no datasets found, returns empty list instead of error
        try:
            datasets = self._execute(workItem).getDatasetsInfoOutput.datasets
        except LegacyXcalarApiStatusException as e:
            if e.message == "Dataset not found":
                return []
            raise e

        ret_val = []
        for dataset in datasets:
            dataset_obj = Dataset(self, name=dataset.datasetName)
            ret_val.append(dataset_obj)

        return ret_val

    def get_dataset(self, dataset_name):
        """
        This method returns a
        :class:`Dataset <xcalar.external.dataset.Dataset>` instance
        representing an existing dataset (with the specified name), with which
        useful tasks can be performed.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> dataset = client.get_dataset(
                    dataset_name="get_demo") # doctest: +SKIP

        :param dataset_name: *Required.* Name of dataset.
        :type dataset_name: str
        :Returns: A :class:`Dataset <xcalar.external.dataset.Dataset>` instance
                which represents the dataset (with the specified name).
        :return_type: :class:`Dataset <xcalar.external.dataset.Dataset>`
        :raises TypeError: if dataset_name is not a str
        """
        if (not isinstance(dataset_name, str)):
            raise TypeError("dataset_name must be str, not '{}'".format(
                type(dataset_name)))

        if (".XcalarDS." not in dataset_name):
            search_name = ".XcalarDS." + dataset_name
        else:
            search_name = dataset_name

        datasets = self.list_datasets(search_name)
        if (len(datasets) == 0):
            raise ValueError("No such dataset: '{}'".format(dataset_name))

        if (len(datasets) > 1):
            raise ValueError(
                "Multiple datasets found matching: '{}'".format(dataset_name))

        return datasets[0]

    # data cannot be a list
    def _thrift_to_json(self, data):
        assert (not isinstance(data, list))
        if (isinstance(data, (int, str, bool))):
            return data

        ret = {}
        for key, val in data.__dict__.items():
            if (key == "parserArgJson"):
                if (val != ''):
                    ret[key] = json.loads(val)
                else:
                    ret[key] = val
            elif (isinstance(val, list)):
                ret[key] = []
                for item in val:
                    ret[key].append(self._thrift_to_json(item))
            else:
                ret[key] = self._thrift_to_json(val)

        return ret

    def create_license(self, license_value):
        """
        This method is used to set the license value for the cluster.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> new_license = open("license file").read()
            >>> client.create_license(license_value=new_license)

        :param license_value: *Required.* License string to use in the creation
                of the initial license
        :type license_value: str
        """
        if (not isinstance(license_value, str)):
            raise TypeError("license_value must be str, not '{}'".format(
                type(license_value)))

        req = CreateRequest()
        req.licenseValue.value = license_value
        res = self._license_service.create(req)

    def get_license(self):
        """
        This method returns a GetResponse structure (as defined in
        License.proto) as a json string containing the current license information
        for the cluster.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> license = client.get_license()

        :Returns: GetResponse json string (see License.proto)
        :return_type: json string
        """

        req = GetRequest()
        res = self._license_service.get(req)
        # Convert protobuf format to something more palatable.
        return MessageToJson(res, including_default_value_fields=True)

    def update_license(self, license_value):
        """
        This method is used to update the license value for the cluster.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> updated_license = open("license file").read()
            >>> client.update_license(license_value=updated_license)

        :param license_value: *Required.* License string to use in the update of
                the cluster license
        :type license_value: str
        """
        if (not isinstance(license_value, str)):
            raise TypeError("license_value must be str, not '{}'".format(
                type(license_value)))

        req = UpdateRequest()
        req.licenseValue.value = license_value
        res = self._license_service.update(req)

    def validate_license(self):
        """
        This method is used to validate that the cluster is in compliance with
        the cluster license.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> valid = client.validate_license()

        :Returns: True (cluster is in compliance with the license)
                  False (cluster is in violation with the license)
        :return_type: boolean
        """

        req = ValidateRequest()
        try:
            res = self._license_service.validate(req)
        except XDPException as e:
            assert (e.statusCode in [
                StatusT.StatusLicInsufficientNodes, StatusT.StatusLicExpired
            ])
            return False

        return res.isLicenseCompliant

    def delete_license(self):
        """
        This method is used to delete the cluster license.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> client.delete_license()
        """

        req = DestroyRequest()
        res = self._license_service.destroy(req)

    def create_session(self, session_name):
        """
        This method creates a session and returns a
        :class:`Session <xcalar.external.session.Session>` instance
        representing this newly created session.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> session = client.create_session(session_name="session_demo")

        :param session_name: *Required.* Unique name that identifies the new session.
        :type session_name: str
        :Returns: A :class:`Session <xcalar.external.session.Session>` instance,
            which represents the newly created session.
        :return_type: :class:`Session <xcalar.external.session.Session>`
        :raises TypeError: Incorrect parameter type
        """
        if (not isinstance(session_name, str)):
            raise TypeError("session_name must be str, not '{}'".format(
                type(session_name)))

        # XXX system workbook per user: SDK-826
        if session_name.startswith(sys_wkbk_name):
            raise ValueError(
                f"Invalid session_name, '{session_name}', "
                f"session_name can't start with '{sys_wkbk_name}'")

        sess_name = xcalar.external.session.SDK_SESSION_NAME_PREFIX + session_name
        workItem = WorkItemSessionNew(
            name=sess_name,
            fork=False,
            userName=self.username,
            userIdUnique=self.user_id)
        result = self._execute(workItem)
        session_id = result.output.outputResult.sessionNewOutput.sessionId

        workItem = WorkItemSessionActivate(sess_name, self.username,
                                           self.user_id)
        self._execute(workItem)

        session_obj = xcalar.external.session.Session(
            client=self,
            session_name=session_name,
            session_id=session_id,
            username=self.username)

        return session_obj

    def get_session(self, session_name):
        """
        This method finds the session with the specified name and returns a
        :class:`Session <xcalar.external.session.Session>` instance
        representing the found session.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client
            >>> session = client.get_session(session_name="session_demo")

        :param session_name: *Required.* Unique name that identifies session to
            search for.
        :type session_name: str
        :Returns: A :class:`Session <xcalar.external.session.Session>` instance,
            which represents the found session.
        :return_type: :class:`Session <xcalar.external.session.Session>`
        :raises TypeError: Incorrect parameter type
        :raises ValueError: if session does not exist,
            or multiple sessions found matching session_name
        """
        if (not isinstance(session_name, str)):
            raise TypeError("session_name must be str, not '{}'".format(
                type(session_name)))

        sessions = self.list_sessions(
            xcalar.external.session.SDK_SESSION_NAME_PREFIX + session_name)
        if (len(sessions) == 0):
            raise ValueError("No such session: '{}'".format(session_name))
        if (len(sessions) > 1):
            raise ValueError(
                "Multiple sessions found matching: '{}'".format(session_name))
        workItem = WorkItemSessionActivate(sessions[0].name, self.username,
                                           self.user_id)
        self._execute(workItem)

        return sessions[0]

    def list_sessions(self, pattern="*"):
        """
        This method returns a list of Sessions matching the search pattern.
        Each element of the list is an instance of
        `:class:`Session <xcalar.external.session.Session>`.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> sessions = client.list_sessions()

        :param pattern: *Optional.* Search pattern. Will only list sessions
            that match the search pattern. Default is "*", which lists all
            sessions.
        :type pattern: str
        :return_type:
            list[:class:`Session <xcalar.external.session.Session>`]
        :raises TypeError: if pattern is not a str
        """
        if (not isinstance(pattern, str)):
            raise TypeError("pattern must be str, not '{}'".format(
                type(pattern)))

        sessions = []
        xce_sessions = self._list_sessions(
            xcalar.external.session.SDK_SESSION_NAME_PREFIX + pattern)
        for xce_session in xce_sessions:
            unprefixed_session_name = xce_session.name[len(
                xcalar.external.session.SDK_SESSION_NAME_PREFIX):]
            # hide the system workbook: SDK-826
            if unprefixed_session_name.startswith(sys_wkbk_name):
                continue
            session_obj = xcalar.external.session.Session(
                client=self,
                session_name=unprefixed_session_name,
                session_id=xce_session.sessionId,
                username=self.username)
            sessions.append(session_obj)

        return sessions

    def destroy_session(self, session_name):
        """
        This method destroys the session with the specified name.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> client.destroy_session(session_name="session_demo")

        :param session_name: *Required.* Unique name that identifies session to
            search for.
        :type session_name: str
        :raises TypeError: Incorrect parameter type
        :raises ValueError: if session does not exist
        """
        if (not isinstance(session_name, str)):
            raise TypeError("session_name must be str, not '{}'".format(
                type(session_name)))

        session = self.get_session(session_name)

        session.destroy()

    def create_udf_module(self, udf_module_name, udf_module_source):
        """
        This method creates a UDF module in the shared namespace for UDFs,
        returning the UDF class object for the newly created UDF module.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> udf_source = "def shared_foo():\\n return 'foo'"
            >>> client.create_udf_module("udf_shared_mod", udf_source)

        :param udf_module_name: *Required.* Name of the UDF module
        :type udf_module_name: str
        :param udf_module_source: *Required.* Source code string for the UDF
        :type udf_module_source: str
        :raises TypeError: if udf_module_name or udf_module_source is not a str
        :raises XcalarApiStatusException: if UDF already exists
        :raises XcalarApiStatusException: if UDF name is invalid
        :NOTE: Valid characters in udf_module_name are: alphanumeric, '-', '_'
        :raises XcalarApiStatusException: if UDF source has invalid syntax
        """
        if (not isinstance(udf_module_name, str)):
            raise TypeError("udf_module_name must be str, not '{}'".format(
                type(udf_module_name)))

        if (not isinstance(udf_module_source, str)):
            raise TypeError("udf_module_source must be str, not '{}'".format(
                type(udf_module_source)))

        # workItem does not need session or user name (it's shared)
        workItem = WorkItemUdfAdd(udf_module_name, udf_module_source)
        try:
            result = self._execute(workItem)
            assert result.moduleName == udf_module_name
        except LegacyXcalarApiStatusException as e:
            if e.status == StatusT.StatusUdfModuleLoadFailed:
                print("udf error:\n{}".format(
                    e.output.udfAddUpdateOutput.error.message))
            raise
        # return object from new UDF_module class
        udf_module_obj = xcalar.external.udf.UDF(
            client=self, workbook=None, udf_module_name=udf_module_name)
        return udf_module_obj

    def get_udf_module(self, udf_module_name):
        """
        This method returns a UDF class object for the named UDF module in the
        cluster's shared space for UDFs.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> shared_udf = client.get_udf_module("udf_shared_mod")

        :param udf_module_name: *Required.* Name of the UDF module
        :type udf_module_name: str
        :raises TypeError: if udf_module_name is not a str
        :raises ValueError: if UDF does not exist or multiple modules match
        """
        if (not isinstance(udf_module_name, str)):
            raise TypeError("udf_module_name must be str, not '{}'".format(
                type(udf_module_name)))

        umos = self.list_udf_modules(modNamePattern=udf_module_name)
        if (len(umos) == 0):
            raise ValueError(
                "No such UDF module: '{}'".format(udf_module_name))
        if (len(umos) > 1):
            raise ValueError("Multiple modules found matching: '{}'".format(
                udf_module_name))
        return umos[0]

    def list_udf_modules(self, modNamePattern="*"):
        """
        This method returns a list of UDF class objects from the cluster's
        shared UDF namespace- one for each UDF module whose name matches the
        specified pattern.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> udf_mods = client.list_udf_modules("*modBar*")
        """
        # the name 'sharedUDFs' must match SharedUDFsDirPath in
        # $XLRDIR/src/include/udf/UserDefinedFunction.h
        udf_full_path = "/sharedUDFs/{}:*".format(modNamePattern)
        workItem = WorkItemListXdfs(udf_full_path, "User-defined*")
        listOut = self._execute(workItem)
        mod_names_list = []
        umod_list = []
        if listOut.numXdfs >= 1:
            for uix in range(0, listOut.numXdfs):
                fq_fname = listOut.fnDescs[uix].fnName
                fq_mname, fname = re.match(r'(.*):(.*)', fq_fname).groups()
                mod_name = os.path.basename(fq_mname)
                if mod_name not in mod_names_list:
                    mod_names_list.append(mod_name)
                    umo = xcalar.external.udf.UDF(
                        client=self, workbook=None, udf_module_name=mod_name)
                    umod_list.append(umo)
        return umod_list

    def get_log_level(self):
        """
        This method returns current log level information.

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.compute.localtypes.log_pb2 import GetLevelResponse
            >>> client = Client()
            >>> response = client.get_log_level()
            >>> print response.logLevel

        :returns: The current log level.

        """
        return self._log_service.getLevel(Empty())

    def set_log_level(self,
                      log_level: XcalarSyslogMsgLevel,
                      log_flush=False,
                      log_flush_level: XcalarSyslogFlushLevel = 0,
                      log_flush_period_sec=0):
        """
        This method sets the log level of the Xcalar that is running.

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.compute.localtypes.log_pb2 import SetLevelRequest
            >>> client = Client()
            >>> client.set_log_level(7)

        """
        request = log_pb2.SetLevelRequest()
        request.log_level = log_level
        request.log_flush = log_flush
        request.log_flush_level = log_flush_level
        request.log_flush_period_sec = log_flush_period_sec
        return self._log_service.setLevel(request)

    def get_memory_usage(self, user_name):
        """
        This method returns memory usage information.

        Example:
            >>> from xcalar.external.client import Client
            >>> from xcalar.compute.localtypes.Memory_pb2 import MemoryUsageRequest
            >>> from xcalar.compute.localtypes.Memory_pb2 import MemoryUsageResponse
            >>> client = Client()
            >>> response = client.get_memory_usage("admin")
            >>> print response.user_memory.num_sessions

        :returns: The memory usage

        """
        request = memory_pb2.GetUsageRequest()
        request.scope.workbook.name.username = user_name
        return self._memory_service.getUsage(request)

    def _list_sessions(self, pattern="*"):
        workItem = WorkItemSessionList(
            pattern=pattern, userName=self.username, userIdUnique=self.user_id)

        # if no workbooks found, returns empty list instead of error
        try:
            return self._execute(workItem).sessions
        except LegacyXcalarApiStatusException as e:
            if e.message == "Session does not exist":
                return []
            raise e

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        del state['next_request_id_lock']
        del state['_socket_send_lock']
        del state['_socket_recv_lock']
        return state

    def __setstate__(self, state):
        # Restore instance attributes.
        self.__dict__.update(state)
        # add unpickable entries back.
        self.next_request_id_lock = threading.Lock()
        self._socket_send_lock = threading.Lock()
        self._socket_recv_lock = threading.Lock()
