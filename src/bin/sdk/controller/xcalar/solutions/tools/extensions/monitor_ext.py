import os
from xcalar.solutions.tools.connector import XShell_Connector

# from xcalar.solutions.singleton import Singleton_Controller
# from urllib.parse import urlparse
from colorama import Style


def dispatcher(line):

    usage = f"""
Usage:
    [SysStat]
    {Style.BRIGHT}monitor {Style.DIM}-n <number>
            {Style.NORMAL}-htop    {Style.DIM}[options]{Style.RESET_ALL}                           - execute htop command on node <n>
            {Style.NORMAL}-lsof    {Style.DIM}[options]{Style.RESET_ALL}                           - execute lsof command on node <n>
            {Style.NORMAL}-mpstat  {Style.DIM}[options]{Style.RESET_ALL}                           - execute mpstat command on node <n>
            {Style.NORMAL}-pidstat {Style.DIM}[options]{Style.RESET_ALL}                           - execute pidstat command on node <n>
            {Style.NORMAL}-ss      {Style.DIM}[options]{Style.RESET_ALL}                           - execute ss command on node <n>
            {Style.NORMAL}-top     {Style.DIM}[options]{Style.RESET_ALL}                           - execute top command on node <n>
            {Style.NORMAL}-vmstat  {Style.DIM}[options]{Style.RESET_ALL}                           - execute vmstat command on node <n>

    [XcalarService]
    {Style.BRIGHT}monitor {Style.DIM}-n <number>
            {Style.NORMAL}-xccli   {Style.DIM}[options]{Style.RESET_ALL}                           - execute xccli

    [Cgroup]
    {Style.BRIGHT}monitor {Style.DIM}-n <number>
            {Style.NORMAL}-systemd-cgls    {Style.DIM}[options]{Style.RESET_ALL}                   - execute systemd-cgls command on node <n>
            {Style.NORMAL}-systemd-cgtop   {Style.DIM}[options]{Style.RESET_ALL}                   - execute systemd-cgtop command on node <n>
            {Style.NORMAL}-systemd-run     {Style.DIM}[options]{Style.RESET_ALL}                   - execute systemd-run command on node <n>
            {Style.NORMAL}-cgroupControllerUtil
                             {Style.DIM}[command] [username|uid] [groupname|gid][group path]{Style.RESET_ALL}
                                                         - cgroup controller utilities

    [Monitor Config]
            {Style.NORMAL}-nodes{Style.RESET_ALL}                                  - list nodes ip

    """
    # clean up injection characters
    line = line.replace('&', ' ')
    line = line.replace(';', ' ')

    tokens = line.split()
    # ---------------------
    # connectivity check
    # ---------------------
    if not (XShell_Connector.self_check_client()
            and XShell_Connector.self_check_controller()):
        return None

    # xshell_connector = XShell_Connector()
    # controller = xshell_connector.get_controller()
    # singleton_controller = Singleton_Controller.getInstance()
    # controller = singleton_controller.get_controller()
    # xcalar_properties = controller.propertiesAdapter.getXcalarProperties()

    try:
        n = tokens[1] if len(tokens) >= 3 else None

        # monitor_nodes = xcalar_properties.nodes
        monitor_nodes = XShell_Connector.get_cluster_nodes()
        hostname = monitor_nodes.get(n)

        arguments = ' '.join(tokens[3::]) if len(tokens) > 3 else ''
        print(arguments)
        # action
        monitor = 'ssh -t'

        # ============
        # [SysStat]
        # ============
        if len(tokens) == 1 and tokens[0] == '-nodes':    # list
            for i, name in monitor_nodes.items():
                print(f'\t{i}:{name}')

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-free':
            command = f"""{monitor} {hostname} 'free {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-htop':
            command = f"""{monitor} {hostname} 'htop {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-iostat':
            command = f"""{monitor} {hostname} 'iostat {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-lsof':
            command = f"""{monitor} {hostname} 'lsof {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-mpstat':
            command = f"""{monitor} {hostname} 'mpstat {arguments}'"""
            print(command)
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-sar':
            command = f"""{monitor} {hostname} 'sar {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-ss':
            command = f"""{monitor} {hostname} '/usr/sbin/ss {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-top':
            command = f"""{monitor} {hostname} 'top {arguments}'"""
            os.system(command)

        elif len(
                tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-pidstat':
            command = f"""{monitor} {hostname} 'pidstat {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-vmstat':
            command = f"""{monitor} {hostname} 'vmstat {arguments}'"""
            os.system(command)

        # ========================
        # XcalarService and tools
        # ========================
        elif (len(tokens) >= 3 and tokens[0] == '-n'
              and tokens[2] == '-xcalar-usrnode.service'):
            command_map = {
                'start':
                    f"""{monitor} {hostname} 'sudo systemctl start xcalar-usrnode.service start'""",
                'stop':
                    f"""{monitor} {hostname} 'sudo systemctl start xcalar-usrnode.service stop'""",
                'restart':
                    f"""{monitor} {hostname} 'sudo systemctl start xcalar-usrnode.service restart'""",
            }
            if arguments in command_map:
                os.system(command_map[arguments])
            else:
                print('[Warning]: Invalid command or argumnets.')

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-xccli':
            command = f"""{monitor} {hostname} "/opt/xcalar/bin/xccli {arguments}" """
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-vmstat':
            command = f"""{monitor} {hostname} 'vmstat {arguments}'"""
            os.system(command)

        # ============
        # [Cgroup]
        # ============
        elif len(tokens
                 ) >= 3 and tokens[0] == '-n' and tokens[2] == '-systemd-cgls':
            command = f"""{monitor} {hostname} 'systemd-cgls {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[
                2] == '-systemd-cgtop':
            command = f"""{monitor} {hostname} 'systemd-cgtop {arguments}'"""
            os.system(command)

        elif len(tokens
                 ) >= 3 and tokens[0] == '-n' and tokens[2] == '-systemd-run':
            command = f"""{monitor} {hostname} 'systemd-run {arguments}'"""
            os.system(command)

        # cgroupControllerUtil
        elif (len(tokens) >= 3 and tokens[0] == '-n'
              and tokens[2] == '-cgroupControllerUtil'):
            command = f"""{monitor} {hostname} 'sudo /opt/xcalar/bin/cgroupControllerUtil.sh {arguments}'"""
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
    def auto_completers(self, event):
        return [
        # '-list',
            '-nodes',
            '-htop',
            '-lsof',
            '-n',
            '-mpstat',
            '-pidstat',
            '-ss',
            '-top',
            '-vmstat',
            '-sar',
            '-systemd-cgls',
            '-systemd-cgtop',
            '-systemd-run',
            '-xccli',
            '-cgroupControllerUtil',
        ]

    ipython.register_magic_function(dispatcher, 'line', magic_name='monitor')
    ipython.set_hook('complete_command', auto_completers, str_key='monitor')
    ipython.set_hook('complete_command', auto_completers, str_key='%monitor')


def unload_ipython_extension(ipython):
    pass
