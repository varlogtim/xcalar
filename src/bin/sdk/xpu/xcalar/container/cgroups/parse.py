ignored_fields = [
    'readOnly', 'cgroupName', 'cgroupProcInfo', 'cgroupPath',
    'cgroupController'
]


# at least one field seems to go
# between having a '\n' and not having one
def string_to_int(arg):
    result = None
    if type(arg) is int:
        result = arg
    elif type(arg) is str:
        result = int(arg.rstrip())
    return result


def string_to_string(arg):
    return arg.rstrip()


def string_to_list(arg):
    return list(filter(None, arg.split('\n')))


def string_to_list_int(arg):
    return [int(l) for l in list(filter(None, arg.split('\n')))]


def tuple_string_to_dict(arg):
    return {
        k: int(v)
        for k, v in
        [l.split(' ') for l in list(filter(None, arg.split('\n')))]
    }


def null_parse(arg):
    return arg


def parser(func_dict, input_dict):
    res = {}
    for k, v in input_dict.items():
        processed_key = k.replace('.', '_')
        try:

            res[processed_key] = func_dict[k](v)
        except Exception:
            res[processed_key] = v
    return res


def readOnly_mem_parser(input_dict):
    return parser(memory_parse_dict, dict(input_dict.items()))


def readOnly_cpu_parser(input_dict):
    return parser(cpu_parse_dict, dict(input_dict.items()))


memory_parse_dict = {
    "memory.kmem.tcp.max_usage_in_bytes": string_to_int,
    "memory.kmem.tcp.failcnt": string_to_int,
    "memory.kmem.tcp.limit_in_bytes": string_to_int,
    "memory.memsw.failcnt": string_to_int,
    "memory.memsw.limit_in_bytes": string_to_int,
    "memory.memsw.max_usage_in_bytes": string_to_int,
    "memory.kmem.max_usage_in_bytes": string_to_int,
    "memory.kmem.failcnt": string_to_int,
    "memory.kmem.limit_in_bytes": string_to_int,
    "memory.oom_control": null_parse,
    "memory.move_charge_at_immigrate": string_to_int,
    "memory.swappiness": null_parse,
    "memory.use_hierarchy": string_to_int,
    "memory.failcnt": string_to_int,
    "memory.soft_limit_in_bytes": null_parse,
    "memory.limit_in_bytes": null_parse,
    "memory.max_usage_in_bytes": string_to_int,
    "cgroup.clone_children": string_to_int,
    "notify_on_release": string_to_int,
    "cgroup.procs": string_to_list_int,
    "tasks": string_to_list_int,
    "cgroup_path": null_parse,
    "cgroup_proc_info": null_parse,
    "readOnly": readOnly_mem_parser,
    "memory.kmem.tcp.usage_in_bytes": string_to_int,
    "memory.memsw.usage_in_bytes": string_to_int,
    "memory.kmem.usage_in_bytes": string_to_int,
    "memory.numa_stat": string_to_list,
    "memory.stat": tuple_string_to_dict,
    "memory.usage_in_bytes": string_to_int,
    "cgroup_name": null_parse,
    "cgroup_controller": null_parse,
    "timestamp": null_parse,
    "cluster_node": null_parse
}

cpu_parse_dict = {
    "cpu.rt_period_us": string_to_int,
    "cpu.rt_runtime_us": string_to_int,
    "cpu.cfs_period_us": string_to_int,
    "cpu.cfs_quota_us": string_to_int,
    "cpu.shares": string_to_int,
    "cpuacct.usage": string_to_int,
    "cgroup.clone_children": string_to_int,
    "notify_on_release": string_to_int,
    "tasks": string_to_list_int,
    "cgroup.procs": string_to_list_int,
    "cgroup_path": null_parse,
    "cgroup_proc_info": null_parse,
    "readOnly": readOnly_cpu_parser,
    "cpu.stat": tuple_string_to_dict,
    "cpuacct.stat": tuple_string_to_dict,
    "cpuacct.usage_percpu": string_to_string,
    "cgroup_name": null_parse,
    "cgroup_controller": null_parse,
    "timestamp": null_parse,
    "cluster_node": null_parse
}


def cgroupMemoryParser(input_dict):
    return parser(memory_parse_dict, input_dict)


def cgroupCpuParser(input_dict):
    return parser(cpu_parse_dict, input_dict)


def cgroupCpuAcct(input_dict):
    return parser(cpu_parse_dict, input_dict)


def cgroupParser(cgController, input_dict):
    controller_parser = {
        'memory': memory_parse_dict,
        'cpu': cpu_parse_dict,
        'cpuacct': cpu_parse_dict
    }
    if cgController not in controller_parser:
        return input_dict
    return parser(controller_parser[cgController], input_dict)


def userFilter(cgController, parsed_input_dict):
    def filterDict(unfiltered_dict):
        return {
            k: v
            for k, v in unfiltered_dict.items()
            if k.startswith(cgController + '.') or k in ignored_fields
        }

    result = filterDict(parsed_input_dict)
    if 'readOnly' in result:
        result['readOnly'] = filterDict(result['readOnly'])
    return result


def rwAttributes(parsed_input_dict):
    return [k for k in parsed_input_dict.keys() if k not in ignored_fields]
