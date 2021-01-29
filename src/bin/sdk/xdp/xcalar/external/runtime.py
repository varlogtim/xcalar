# Copyright 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

from enum import Enum

from xcalar.compute.coretypes.LibApisCommon.ttypes import (
    RuntimeTypeT,
    XcalarApiRuntimeSetParamInputT,
    XcalarApiSchedParamT,
)

from xcalar.external.LegacyApi.WorkItem import (
    WorkItemRuntimeSetParam, WorkItemRuntimeGetParam, WorkItemSetConfigParam)


class Runtime:
    """
    Xcalar runtime configuration.

    Runtime by default will have 3 schedulers enabled.
    -----------------------------------------------------------------------------
    SchedId      Type            CpusReservedInPct
    -----------------------------------------------------------------------------
    Sched0       Througphput                 100
    Sched1       Latency/Throughput          100
                 (default Latency)
    Sched2       Latency                     100
    -----------------------------------------------------------------------------
    """

    class Scheduler(Enum):
        Sched0 = "Scheduler-0"
        Sched1 = "Scheduler-1"
        Sched2 = "Scheduler-2"
        Immediate = "Immediate"

    class RuntimeType(Enum):
        Throughput = RuntimeTypeT.Throughput
        Latency = RuntimeTypeT.Latency
        Immediate = RuntimeTypeT.Immediate

    def __init__(self, client):
        self._client = client
        self.get_sched_values()

    @staticmethod
    def get_dataflow_scheds():
        scheds = []
        # "Immediate" Scheduler cannot be used to Scheduler DFs.
        for sched in list(Runtime.Scheduler.__members__.values()):
            if sched.value is not 'Immediate':
                scheds.append(sched)
        return scheds

    def get_sched_values(self):
        """
        This method returns the current sched values cluster is set to.

        Example:
            >>> from xcalar.external.client import Client
            >>> client = Client()
            >>> runtime = client.runtime()
            >>> runtime.get_sched_values()
            {'Sched0': {'runtime_type': 'Throughput', 'cpus_reserved_percent': 100},
             'Sched1': {'runtime_type': 'Latency', 'cpus_reserved_percent': 100},
             'Sched2': {'runtime_type': 'Latency', 'cpus_reserved_percent': 100},
             'Immediate': {'runtime_type': 'Immediate', 'cpus_reserved_percent': 400}}

        :Returns: a dictionary structure with sched name as key and value as
         another dictionary with runtime_type and cpus_reserved_percent key-value entries
        :return_type: dict
        """
        workItem = WorkItemRuntimeGetParam()
        scheds = self._client._execute(workItem)
        self._sched_values = {}
        for sched_info in scheds.schedParams:
            sched = self.Scheduler(sched_info.schedName)
            runtime_type = self.RuntimeType(sched_info.runtimeType)
            cpus_reserved_percent = sched_info.cpusReservedInPercent
            self._sched_values[sched.name] = {
                "runtime_type": runtime_type.name,
                "cpus_reserved_percent": cpus_reserved_percent
            }
        return self._sched_values

    def sched_value(self, sched_name, runtime_type, cpus_reserved_percent):
        """
        This method allows to change sched values but won't update the cluster
        unless you do update_sched_values call.

        :param sched_name: The sched name to change the values of.
        :type sched_name: A :class:`Scheduler <xcalar.external.runtime.Runtime.Scheduler>` instance.
        :param runtime_type: The runtime type to assign in to the sched name.
        :type runtime_type: A :class:`RuntimeType <xcalar.external.runtime.Runtime.RuntimeType>` instance.
        :param cpus_reserved_percent: cpus reverved for this sched in percentage
        :type cpus_reserved_percent: int or float
        """
        if not isinstance(sched_name, self.Scheduler):
            raise TypeError(
                "sched_name must be Runtime.Scheduler enum, not '{}'".format(
                    type(sched_name)))
        if not isinstance(runtime_type, self.RuntimeType):
            raise TypeError(
                "runtime_type must be Runtime.RuntimeType enum, not '{}'".
                format(type(runtime_type)))
        if not isinstance(cpus_reserved_percent, (int, float)):
            raise TypeError(
                "cpus_reserved_percent must be int or float type, not '{}'".
                format(type(cpus_reserved_percent)))

        if (sched_name == self.Scheduler.Sched0
                and runtime_type == self.RuntimeType.Latency) or (
                    sched_name == self.Scheduler.Sched2
                and runtime_type == self.RuntimeType.Throughput) or (
		    sched_name == self.Scheduler.Immediate
                and runtime_type == self.RuntimeType.Immediate):
            raise ValueError(
                "Invalid runtime type '{}' for given sched '{}'".format(
                    runtime_type, sched_name))
        self._sched_values[sched_name.name] = {
            "runtime_type": runtime_type.name,
            "cpus_reserved_percent": cpus_reserved_percent
        }
        return self

    def update_sched_values(self):
        """
        This method updates the sched values on the cluster, with sched inputs
        obtained using sched_value method.
        """
        rt_set_params = XcalarApiRuntimeSetParamInputT()
        rt_set_params.schedParams = []
        for sched_name, sched_vals in self._sched_values.items():
            sched_param = XcalarApiSchedParamT()
            sched_param.schedName = Runtime.Scheduler[sched_name].value
            sched_param.runtimeType = Runtime.RuntimeType[
                sched_vals["runtime_type"]].value
            sched_param.cpusReservedInPercent = sched_vals[
                "cpus_reserved_percent"]
            rt_set_params.schedParams.append(sched_param)
        req = WorkItemRuntimeSetParam(rt_set_params)
        self._client._execute(req)

    def set_mixed_mode_min_cores(self, num_cores):
        """
        This method configures the cluster with the minimum cores for mixed mode.
        :param num_cores: minimum cores for mixed mode setup
        :type num_cores: int
        """
        req = WorkItemSetConfigParam("RuntimeMixedModeMinCores", num_cores)
        self._client._execute(req)
