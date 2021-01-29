import grpc
import os
import sys
import logging
import time
from grpc import StatusCode
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from xcalar.external.exceptions import XDPException
from xcalar.compute.localtypes.Version_pb2_grpc import VersionStub
from xcalar.compute.localtypes.Echo_pb2 import EchoRequest
from xcalar.compute.localtypes.Echo_pb2_grpc import EchoStub
from xcalar.compute.util.config import detect_config
from xcalar.compute.coretypes.Status.ttypes import StatusT
import xcalar.container.xpu_host as xpu_host

logger = logging.getLogger()

empty = google_dot_protobuf_dot_empty__pb2.Empty()

#
# We keep one channel per process
#
channels = {}

grpc_server_addr = None


def get_port():
    node_id = 0
    if (xpu_host.is_xpu()):
        node_id = int(xpu_host.get_node_id(xpu_host.get_xpu_id()))
    grpc_server_port = 51234
    config_dict = detect_config().all_options
    if ('Constants.GrpcServerPort' in config_dict):
        grpc_server_port = int(config_dict['Constants.GrpcServerPort'])
    return grpc_server_port + node_id


def get_addr():
    global grpc_server_addr
    grpc_server_port = get_port()
    grpc_server_addr = 'localhost:' + str(grpc_server_port)


def get_channel():
    global grpc_server_addr
    if (grpc_server_addr == None):
        get_addr()

    pid = os.getpid()
    if (not pid in channels):
        logger.debug(f'Creating new channel for {pid}')
        channels[pid] = grpc.insecure_channel(grpc_server_addr)
        #
        # Bug ENG-9034: For some reason we get an exception the first time that
        # we try to connect to the backend from a new python process.  It
        # doesn't happen in the primary process - only in processes that are
        # created by the main process.  The creating of an insecure channel
        # doesn't actually connect so as a work around we send an echo to force
        # the error and if it doesn't work we will create a new channel. I have
        # been unable to recreate this problem in a multiprocessing test case but
        # it shows up in test_operators.py.
        #
        try:
            EchoStub(channels[pid]).EchoMessage(EchoRequest(echo="Hello"))
        except Exception as e:
            logger.debug(
                f'Exception in process {pid} on echo so recreating channel')
            channels[pid] = grpc.insecure_channel(grpc_server_addr)
    return channels[pid]


#
# invoke a grpc function and catch a grpc exception and map it to
# XDPException
#
def invoke(stub_name, func_name, *args):
    logger.debug(f'Process {os.getpid()} invoking {func_name}')
    stub = stub_name(get_channel())
    method_to_call = getattr(stub, func_name)
    exc = None
    try:
        return method_to_call(*args)
    except grpc.RpcError as rpcError:
        logger.debug(
            f'RpcError: code={rpcError.code()} details={rpcError.details()}')
        status_code = None
        status_message = None
        if (rpcError.code() == StatusCode.UNAVAILABLE):
            logger.warning(f'Server @{grpc_server_addr} is unavailable')
            status_code = StatusT.StatusHostDown
            status_message = rpcError.details()
        elif (rpcError.code() == StatusCode.ABORTED):
            strs = rpcError.details().split(":")
            status_code = (int)(strs[0])
            status_message = strs[1]
        else:
            raise rpcError

        #
        # Don't raise the new exception here to avoid Python spew about raising
        # another exception while processing the grpc exception.
        #
        raise XDPException(status_code, status_message) from None


#
# Return the version of Xcalar that is running.
#
def get_version():
    return invoke(VersionStub, "GetVersion", empty)
