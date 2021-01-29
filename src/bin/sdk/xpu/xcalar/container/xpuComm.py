# Copyright 2017 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import xcalar.container.parent as xce
import xcalar.container.context as ctx
import xcalar.container.xpu_host as xpu_host
import multiprocessing
import logging
import sys
import pickle

num_cores = multiprocessing.cpu_count()

logger = logging.getLogger('xcalar')


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


#
# Send "payload_string" (a string/byte stream) to a XPU with id "xpu_id"
# Since the payload is considered a string, NULL termination is assumed. If
# this is undesirable, the interface will need to take a length param.
#
# This call is synchronous, in that, after the call returns without exception,
# the caller can assume the payload has been deposited into the target XPU, and
# the caller can now free the memory used to hold payload_string. Note that the
# string is deposited and received in FIFO order.
#
# Note: Payloads are tagged internally to differentiate between different users
# of the underlying send/recv infrastructure.
#
# The externally visible APIs, send_msg, send_msg_fanout and recv_msg, which can be
# invoked by users/apps, use the "ctag_user" tag when invoking the internal APIs
# _send_msg, _send_msg_fanout and _recv_msg respectively.
#
# The externally visible barrier API (barrier_wait), also uses the internal
# send/recv APIs, and it must invoke the same internal APIs using a different
# tag, "ctag_barr".
#
# Note that the names of the tags "ctagUser" and "ctagBarr" must be kept in sync
# with their use in UdfPyXcalar.cpp.


def send_msg(xpu_id, payload_string):
    _send_msg("ctagUser", xpu_id, payload_string)


def _send_msg(xpu_comm_tag_str, xpu_id, payload_string):
    assert xpu_id != ctx.get_xpu_id(
    )    # we shouldn't be sending to ourselveds
    return _send_fanout(xpu_comm_tag_str, [(xpu_id, payload_string)])


# Set up buffer cache for fanout sends. Initialized with the total number of
# BC SHM buffers needed for all payloads to all destination XPUs. So there's
# only one trip from XPU to XCE for all buffers for all destinations in the
# fanout request.
class MessageBatch():
    def __init__(self, tot_bufs):
        self._xce_client = xce.Client()
        self.tot_bufs = tot_bufs
        self.empty_buffers = []
        self.full_buffers = []
        self.send_list = []

    def get_buf(self):
        if len(self.empty_buffers) == 0:
            self.empty_buffers = self._xce_client.get_output_buffers(
                self.tot_bufs)
        return self.empty_buffers.pop()

    # Each buf is assumed to have a payload of "page_size" bytes. So no length
    # param is needed. All bufs must obviously be fully filled except last one
    def put_buf(self, buf):
        self.full_buffers.append(buf)

    # Last buf's payload (of size buflen) may be < page_size bytes
    # Append last buf, prep the (dst_xpu, buflist) tuple and append to send_list
    # tot_bufs_for_dst is needed to assert that full_buffers has a valid length
    def put_last_buf(self, buf, buflen, dst_xpu, tot_bufs_for_dst):
        page_size = self._xce_client.get_xdb_page_size()
        self.full_buffers.append(buf)
        assert len(self.full_buffers) == tot_bufs_for_dst

        o_buffs = [(addr, page_size) for addr in self.full_buffers[:-1]]
        o_buffs.append((self.full_buffers[-1], buflen))
        self.full_buffers = []
        self.send_list.append((dst_xpu, o_buffs))

    def send_all_bufs(self, src_xpu_id):
        self._xce_client.send_output_buffers_fanout(src_xpu_id, self.send_list,
                                                    self.empty_buffers)


#
# Send to a list of destination XPUs, a different payload for each destination.
# Argument: a list of (xpu_id, payload_string) tuples: [ (d1, p1), (d2, p2), ...]
# The interface is synchronous but the assumption is that the payloads will be
# sent concurrently (not serially) to the different destinations, to prevent
# the severe performance degradation that would occur otherwise (if the sends
# occurred serially, the total time would include the sum of all round-trips)
#
def send_fanout(messages):
    _send_fanout("ctagUser", messages)


def _send_fanout(xpu_comm_tag_str, messages):
    xce_client = xce.Client()
    page_size = xce_client.get_xdb_page_size()
    ret = None
    n_bufs_total = 0
    this_xpu_id = ctx.get_xpu_id()
    this_node_id = ctx.get_node_id(this_xpu_id)
    # Compute n_bufs needed for each destination, and add them into n_bufs_total
    for message in messages:
        payload_size = len(message[1])
        n_bufs = xpu_host.xpu_compute_num_xdb_pages_required_on_wire(
            page_size, message[1])
        n_bufs_total += n_bufs
        logger.debug("xpu {} -> xpu {} sent len {} npages {}".format(
            this_xpu_id, message[0], payload_size, n_bufs))
        assert this_xpu_id != message[0]

    # n_bufs_total is used as a hint to bulk-allocate xdbPages, so that we
    # avoid xdbPages 1 at a time. This is because each allocation request is
    # potentially a lengthy XPU-XCE communication
    xc_bC = MessageBatch(n_bufs_total)

    xpu_host.xpu_send_string_fanout(xpu_comm_tag_str, page_size, xc_bC.get_buf,
                                    xc_bC.put_buf, xc_bC.put_last_buf,
                                    xc_bC.send_all_bufs, messages)
    return ret


def recv_msg():
    return _recv_msg("ctagUser")


# Return a byte stream / string from xpu recv FIFO queue. Will block if the
# queue is empty (and wakes up in response to a send_msg())
def _recv_msg(xpu_comm_tag_str):
    this_xpu_id = ctx.get_xpu_id()
    this_node_id = ctx.get_node_id(this_xpu_id)
    try:
        recv_string = xpu_host.xpu_recv_string(xpu_comm_tag_str)
        logger.debug("xpu {} <- received len {}".format(
            this_xpu_id, len(recv_string)))
    except Exception as e:
        logger.exception(
            "Node {} caught recv_msg exception".format(this_node_id))
        raise
    return recv_string


#
# Synchronize across all XPUs in the XPU cluster: all XPUs must call
# barrier_wait() and they return from this call only when they've
# all entered the barrier.
#
# The following example schematic describes the main barrier logic using send
# and receive APIs:
#
#  In this example, there are three nodes, with 9 XPUs (X0 - X8).
#  LA = local aggregator, GA = global aggregator
#
#  X0 and X3 are local aggregators, and X6 is the global aggregator.
#
#  Barrier entry goes through the following sequence of steps from A to D:
#
#  (A) The slaves (X1, X2, X4, X5, X7, X8) send their Ids to their local
#      aggregators (LA-1, LA-1, GA) respectively, signaling barrier entry, and
#      then block waiting to hear back from the local aggregators
#
#  (B) The local aggregators (LA-0, LA-1) once they've received their respective
#      slaves' entries, send their Ids to GA, and then block waiting to hear
#      back from the global aggregator
#
#  (C) Once GA receives the local aggregators' Ids, *and* its own slaves' Ids,
#      GA knows that the entire cluster of XPUs is now ready to enter the
#      barrier, and so it sends completion to its slaves, and the local
#      aggregators LA-0 and LA-1
#
#  (D) Once the local aggregators receive the GA's Id, they send a message to
#      their slaves, and on successful transmission, they enter the barrier.
#      The slaves, on receiving their local aggregator's message, also enter the
#      barrier. Barrier is now done.
#
#       Node 0                  Node 1                    Node 2
#
#    X0    X1    X2           X3    X4    X5        X6    X7    X8
#    ^     ^     ^            ^     ^     ^         ^     ^     ^
#    |     |     |            |     |     |         |     |     |
#   LA-0   S     S           LA-1   S     S         GA    S     S
#    <-----|     |             <----|     |         | <---|
#    <-----------| (A)         <-- (A) ---|         | <---------| (A)
#                                                   |
#    |--- (B) ------------------------------------> |
#                             |--- (B) -----------> |
#                                                   |
#                             |<--- (C) ------------|---->|
#    <------------------------- (C) ----------------|---------->|
#    |---->|                  |---->|
#    |--- (D) -->|            |---------->|         |
#    |     |     |            |     |     |         |     |     |
#    |     |     |            |     |     |         |     |     |
#    V     V     V            V     V     V         V     V     V
#    ---------------------  BARRIER DONE ---------------------------
#


#
# Barrier: incorporates the internal config and methods needed for the
# barrier
#
class Barrier():
    def __init__(self):
        self.this_xpu_id = ctx.get_xpu_id()
        self.this_node_id = ctx.get_node_id(self.this_xpu_id)
        self.num_xpus_in_node = (
            xpu_host.get_xpu_end_id_in_node(self.this_node_id) -
            xpu_host.get_xpu_start_id_in_node(self.this_node_id)) + 1
        self.num_nodes = ctx.get_node_count()
        self.num_xpus = ctx.get_xpu_cluster_size()

    # Given a XPU's id, return its barrier's local aggregator XPU id
    def local_aggregate(self, xpu_id):
        return xpu_host.get_xpu_start_id_in_node(ctx.get_node_id(xpu_id))

    # Return the XPU id of my XPU cluster's global barrier aggregator
    def global_aggregate(self):
        return self.local_aggregate(self.num_xpus - 1)

    # Return true if the caller XPU is a slave XPU in the barrier logic
    def is_slave(self):
        return (self.this_xpu_id != self.local_aggregate(self.this_xpu_id)
                and self.this_xpu_id != self.global_aggregate())

    # Return true if the caller XPU is a local aggregator in the barrier logic
    def is_local_aggregator(self):
        return self.this_xpu_id == self.local_aggregate(self.this_xpu_id)

    # Return true if caller XPU is the global aggregator in the barrier logic
    def is_global_aggregator(self):
        return self.this_xpu_id == self.global_aggregate()


#
# The barrier_wait() API which executes the logic in the above schematic
#
# XXX: TODO: Remove validation of _recv_msg() returns after sufficient soak time
# The code to be removed has the comment: "validate return from _recv_msg"
#


def barrier_wait():
    xc_barr = Barrier()
    logger.debug("barrier_wait xpuId {} numNodes {} nodeId {}".format(
        xc_barr.this_xpu_id, xc_barr.num_nodes, xc_barr.this_node_id))
    # Slave:
    if xc_barr.is_slave():
        xpu_la = xc_barr.local_aggregate(xc_barr.this_xpu_id)
        assert (xpu_la != xc_barr.this_xpu_id)
        _send_msg("ctagBarr", xpu_la, pickle.dumps(xc_barr.this_xpu_id))
        # validate return from _recv_msg
        msg = _recv_msg("ctagBarr")
        msg_la = pickle.loads(msg)
        if not msg_la == xpu_la:
            raise RuntimeError("barrier_wait failure! slave {} received {} "
                               "which isn't the Local Aggr xpu ID {}".format(
                                   xc_barr.this_xpu_id, msg_la, xpu_la))

    elif xc_barr.is_local_aggregator():
        if xc_barr.is_global_aggregator():

            # Global Aggregator:

            # First, receive from all slaves and all local aggregators
            n_xpus_to_aggr = (xc_barr.num_xpus_in_node - 1) + (
                xc_barr.num_nodes - 1)
            sender_xpus = []    # for validation
            sender_xpus.extend([None] * xc_barr.num_xpus)    # for validation
            for i in range(n_xpus_to_aggr):
                msg = _recv_msg("ctagBarr")
                sender_xpu_id = pickle.loads(msg)
                # validate return from _recv_msg
                if sender_xpu_id >= xc_barr.num_xpus:
                    raise RuntimeError(
                        "barrier_wait failure! GA received "
                        "invalid XPU Id {}".format(sender_xpu_id))
                else:
                    sender_xpus[sender_xpu_id] = 1

            # Then, send to all slaves and local aggregators by building the
            # fanout send_list for all slaves and all local aggregators. Validate
            # the receipts in sender_xpus[] collected above, as the send_list is
            # built

            send_list = []

            # Build the list for slaves
            for i in range(1, xc_barr.num_xpus_in_node):
                # validate return from _recv_msg
                if sender_xpus[xc_barr.this_xpu_id + i] == 0:
                    raise RuntimeError(
                        "barrier_wait failure! GA didn't "
                        "receive from slave {}".format(xc_barr.this_xpu_id +
                                                       i))
                else:
                    assert (xc_barr.this_xpu_id + i != xc_barr.this_xpu_id)
                    msg = pickle.dumps(xc_barr.this_xpu_id)
                    send_list.append((xc_barr.this_xpu_id + i, msg))

            # Build the list for local aggregators on all other nodes
            for i in range(xc_barr.num_nodes - 1):
                # validate return from _recv_msg
                if sender_xpus[xpu_host.get_xpu_start_id_in_node(i)] == 0:
                    raise RuntimeError(
                        "barrier_wait failure! GA didn't "
                        "receive from LA {}".format(
                            xpu_host.get_xpu_start_id_in_node(i)))
                else:
                    assert (xpu_host.get_xpu_start_id_in_node(i) !=
                            xc_barr.this_xpu_id)
                    msg = pickle.dumps(xc_barr.this_xpu_id)
                    send_list.append((xpu_host.get_xpu_start_id_in_node(i),
                                      msg))

            # Finally, issue the fanout send to all slaves and local aggregators
            if len(send_list) != 0:
                _send_fanout("ctagBarr", send_list)
        else:
            # Local aggregator

            # sender_xpus[] is needed to validate return from _recv_msg
            sender_xpus = []
            sender_xpus.extend([None] * xc_barr.num_xpus_in_node)

            # First, receive from all slaves on my node, and build receipts in
            # sender_xpus[] for subsequent validation

            for i in range(1, xc_barr.num_xpus_in_node):
                msg = _recv_msg("ctagBarr")
                sender_xpu_id = pickle.loads(msg)
                # validate return from _recv_msg
                loc_xpu_id = ctx.get_local_xpu_id(sender_xpu_id)
                if loc_xpu_id >= xc_barr.num_xpus_in_node or loc_xpu_id == 0:
                    raise RuntimeError("barrier_wait failure! LA {} "
                                       "received from bad sender {}".format(
                                           xc_barr.this_xpu_id, sender_xpu_id))
                else:
                    sender_xpus[loc_xpu_id] = 1

            # Next, validate the returns from _recv_msg
            for i in range(1, xc_barr.num_xpus_in_node):
                if sender_xpus[i] == 0:
                    raise RuntimeError("barrier_wait failure! LA {} didn't "
                                       "receive from slave {}".format(
                                           xc_barr.this_xpu_id,
                                           xc_barr.this_xpu_id + i))

            # Then, send to the cluster's global aggregator
            my_ga = xc_barr.global_aggregate()
            assert (my_ga != xc_barr.this_xpu_id)
            _send_msg("ctagBarr", my_ga, pickle.dumps(xc_barr.this_xpu_id))

            # Wait for global aggregator to respond
            msg = _recv_msg("ctagBarr")
            # validate return from _recv_msg
            sender_ga = pickle.loads(msg)
            if not sender_ga == my_ga:
                raise RuntimeError("barrier_wait failure! LA received "
                                   "{} not GA {}".format(msg, my_ga))

            # Finally, tell all my slaves that barrier's done: first build
            # the send_list and then issue the fanout send to my slaves
            send_list = []

            for i in range(1, xc_barr.num_xpus_in_node):
                assert (xc_barr.this_xpu_id + i != xc_barr.this_xpu_id)
                msg = pickle.dumps(xc_barr.this_xpu_id)
                send_list.append((xc_barr.this_xpu_id + i, msg))

            if len(send_list) != 0:
                _send_fanout("ctagBarr", send_list)
    else:
        raise RuntimeError("barrier_wait failure! Neither slave, nor aggr")
    return
