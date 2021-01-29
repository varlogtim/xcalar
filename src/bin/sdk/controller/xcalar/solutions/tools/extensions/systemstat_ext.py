import os
from xcalar.solutions.tools.utilities.nodes import get_nodes_from_cfg
from colorama import Style

def dispatcher(line):

    usage = f"""
Usage:
    [SysStat]
    {Style.BRIGHT}systemstat {Style.DIM}-n <node_id>
            {Style.NORMAL}-htop    {Style.DIM}[options]{Style.RESET_ALL}                           - execute htop command on node <n>
            {Style.NORMAL}-iotop   {Style.DIM}[options]{Style.RESET_ALL}                           - execute iotop command on node <n>
            {Style.NORMAL}-lsof    {Style.DIM}[options]{Style.RESET_ALL}                           - execute lsof command on node <n>
            {Style.NORMAL}-mpstat  {Style.DIM}[options]{Style.RESET_ALL}                           - execute mpstat command on node <n>
            {Style.NORMAL}-pidstat {Style.DIM}[options]{Style.RESET_ALL}                           - execute pidstat command on node <n>
            {Style.NORMAL}-sysstat {Style.DIM}[options]{Style.RESET_ALL}                           - execute sysstat command on node <n>
            {Style.NORMAL}-ss      {Style.DIM}[options]{Style.RESET_ALL}                           - execute ss command on node <n>
            {Style.NORMAL}-strace  {Style.DIM}[options]{Style.RESET_ALL}                           - execute strace command on node <n>
            {Style.NORMAL}-top     {Style.DIM}[options]{Style.RESET_ALL}                           - execute top command on node <n>
            {Style.NORMAL}-vmstat  {Style.DIM}[options]{Style.RESET_ALL}                           - execute vmstat command on node <n>

    [Cgroup]
    {Style.BRIGHT}systemstat {Style.DIM}-n <node_id>
            {Style.NORMAL}-systemd-cgls    {Style.DIM}[options]{Style.RESET_ALL}                   - execute systemd-cgls command on node <n>
            {Style.NORMAL}-systemd-cgtop   {Style.DIM}[options]{Style.RESET_ALL}                   - execute systemd-cgtop command on node <n>
            {Style.NORMAL}-systemd-run     {Style.DIM}[options]{Style.RESET_ALL}                   - execute systemd-run command on node <n>
            {Style.NORMAL}-cgroupControllerUtil {Style.DIM}[command] [username|uid] [groupname|gid] [group path]{Style.RESET_ALL}
                                                        - cgroup controller utilities

    [Xcalar tool]
    {Style.BRIGHT}systemstat {Style.DIM}-n <node_id>
            {Style.NORMAL}-xccli     {Style.DIM}   [options]{Style.RESET_ALL}                    - execute xccli comand on node <n>
            {Style.NORMAL}-reset_libstats {Style.DIM} --non_cumulative --non_hwm {Style.RESET_ALL} {Style.BRIGHT}
                            - To reset stats in all nodes, set <node_id> to -1.
                            - non_cumulative is optional. Use this flag if you want to only reset hwm stats.
                            - non_hwm is optional. Use this flag if you want to only reset cumulative stats.

    [Config]
    {Style.BRIGHT}systemstat {Style.DIM}
            {Style.NORMAL}-nodes     {Style.DIM}{Style.RESET_ALL}                                - list nodes ip
    """

    # clean up injection characters
    line = line.replace('&', ' ')
    line = line.replace(';', ' ')

    tokens = line.split()

    try:
        if len(tokens) == 0:
            print(usage)
            return

        n = int(tokens[1]) if len(tokens) > 1 else None

        # By default reset_stats resets all the libstats in node X. If the node_id
        # is set to -1, this indicates that libstats will be reset on all nodes.
        # Example usage:
        # "systemstat -n -1 reset_libstats" will reset stats on all nodes.
        # "systemstat -n 1 reset_libstats --non_cumulative" will only reset hwm stats in node
        if len(tokens
               ) >= 3 and tokens[0] == '-n' and tokens[2] == '-reset_libstats':
            # reset libstats
            from xcalar.solutions.tools.connector import XShell_Connector
            # connectivity check
            if not (XShell_Connector.self_check_client()):
                return None

            client = XShell_Connector.get_client()
            node_id = None if n < 0 else n
            cumulative = True
            hwm = True

            if "--non_cumulative" in tokens:
                cumulative = False

            if "--non_hwm" in tokens:
                hwm = False

            # execute
            client.reset_libstats(node_id, cumulative, hwm)
            return

        remote_nodes = get_nodes_from_cfg()

        # ============
        # [SysStat]
        # ============
        if len(tokens) == 1 and tokens[0] == '-nodes':
            for i, name in remote_nodes.items():
                print(f'\t{i}:{name}')
            return

        remote = 'ssh -o LogLevel=QUIET -t'
        hostname = remote_nodes.get(str(n))
        arguments = ' '.join(tokens[3::]) if len(tokens) > 3 else ''

        if len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-free':
            command = f"""{remote} {hostname} 'free {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-htop':
            command = f"""{remote} {hostname} 'htop {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-iotop':
            command = f"""{remote} {hostname} 'sudo iotop {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-lsof':
            command = f"""{remote} {hostname} 'lsof {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-mpstat':
            command = f"""{remote} {hostname} 'mpstat {arguments}'"""
            print(command)
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-sar':
            command = f"""{remote} {hostname} 'sar {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-ss':
            command = f"""{remote} {hostname} '/usr/sbin/ss {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-strace':
            command = f"""{remote} {hostname} 'strace {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-top':
            command = f"""{remote} {hostname} 'top {arguments}'"""
            os.system(command)

        elif len(
                tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-pidstat':
            command = f"""{remote} {hostname} 'pidstat {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-vmstat':
            command = f"""{remote} {hostname} 'vmstat {arguments}'"""
            os.system(command)

        # ========================
        # XcalarService and tools
        # ========================
        elif (len(tokens) >= 3 and tokens[0] == '-n'
              and tokens[2] == '-xcalar-usrnode.service'):
            command_map = {
                'start':
                    f"""{remote} {hostname} 'sudo systemctl start xcalar-usrnode.service start'""",
                'stop':
                    f"""{remote} {hostname} 'sudo systemctl start xcalar-usrnode.service stop'""",
                'restart':
                    f"""{remote} {hostname} 'sudo systemctl start xcalar-usrnode.service restart'""",
            }
            if arguments in command_map:
                os.system(command_map[arguments])
            else:
                print('[Warning]: Invalid command or argumnets.')

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-xccli':
            command = f"""{remote} {hostname} "/opt/xcalar/bin/xccli {arguments}" """
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-vmstat':
            command = f"""{remote} {hostname} 'vmstat {arguments}'"""
            os.system(command)

        # ============
        # [Cgroup]
        # ============
        elif len(tokens
                 ) >= 3 and tokens[0] == '-n' and tokens[2] == '-systemd-cgls':
            command = f"""{remote} {hostname} 'systemd-cgls {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[
                2] == '-systemd-cgtop':
            command = f"""{remote} {hostname} 'systemd-cgtop {arguments}'"""
            os.system(command)

        elif len(tokens
                 ) >= 3 and tokens[0] == '-n' and tokens[2] == '-systemd-run':
            command = f"""{remote} {hostname} 'systemd-run {arguments}'"""
            os.system(command)

        # cgroupControllerUtil
        elif (len(tokens) >= 3 and tokens[0] == '-n'
              and tokens[2] == '-cgroupControllerUtil'):
            command = f"""{remote} {hostname} 'sudo /opt/xcalar/bin/cgroupControllerUtil.sh {arguments}'"""
            os.system(command)
        else:
            print(usage)
    except AttributeError as ex:
        print(f'{ex}')
    except KeyboardInterrupt:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
        return
    except Exception as e:
        print(e)


def load_ipython_extension(ipython, *args):
    # use ipython.register_magic_function
    #   to register "dispatcher" function
    #   make magic_name = 'http'

    def auto_completers(self, event):
        return [
            '-htop',
            '-lsof',
            '-mpstat',
            '-n',
            '-nodes',
            '-pidstat',
            '-ss',
            '-strace',
            '-top',
            '-vmstat',
            '-sar',
            '-systemd-cgls',
            '-systemd-cgtop',
            '-systemd-run',
            '-xccli',
            '-reset_libstats'
            '-cgroupControllerUtil',
        ]

    ipython.register_magic_function(
        dispatcher, 'line', magic_name='systemstat')
    ipython.set_hook('complete_command', auto_completers, str_key='systemstat')
    ipython.set_hook(
        'complete_command', auto_completers, str_key='%systemstat')


def unload_ipython_extension(ipython):
    pass
