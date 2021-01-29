import os
from xcalar.solutions.tools.connector import XShell_Connector

# from xcalar.solutions.singleton import Singleton_Controller
# from urllib.parse import urlparse
from colorama import Style


def dispatcher(line):

    usage = f"""
Usage:
    [SysStat]
    {Style.BRIGHT}remote {Style.DIM}-n <number>  {Style.NORMAL}-htop    {Style.DIM}[options]{Style.RESET_ALL}                           - show vmnode <n> htop
                        {Style.NORMAL}-iostat  {Style.DIM}[options]{Style.RESET_ALL}                           - show vmnode <n> iostat
                        {Style.NORMAL}-iotop   {Style.DIM}[options]{Style.RESET_ALL}                           - show vmnode <n> iotop
                        {Style.NORMAL}-lsof    {Style.DIM}[options]{Style.RESET_ALL}                           - show vmnode <n> lsof
                        {Style.NORMAL}-list_nodes{Style.RESET_ALL}                                  - list vmnodes
                        {Style.NORMAL}-mpstat  {Style.DIM}[options]{Style.RESET_ALL}                           - show vmnode <n> mpstat
                        {Style.NORMAL}-perf    {Style.DIM}[optinos]{Style.RESET_ALL}                           - show vmnode <n> perf
                        {Style.NORMAL}-pidstat {Style.DIM}[options]{Style.RESET_ALL}                           - show vmnode <n> pidstat
                        {Style.NORMAL}-sysstat {Style.DIM}[options]{Style.RESET_ALL}                           - show vmnode <n> sysstat
                        {Style.NORMAL}-ss      {Style.DIM}[options]{Style.RESET_ALL}                           - show vmnode <n> ss
                        {Style.NORMAL}-strace  {Style.DIM}[options]{Style.RESET_ALL}                           - show vmnode <n> strace
                        {Style.NORMAL}-top     {Style.DIM}[options]{Style.RESET_ALL}                           - show vmnode <n> top
                        {Style.NORMAL}-vmstat  {Style.DIM}[options]{Style.RESET_ALL}                           - show vmnode <n> vmstat


    [Cgroup]
    {Style.BRIGHT}remote {Style.DIM}-n <number>  {Style.NORMAL}-systemd-cgls    {Style.DIM}[options]{Style.RESET_ALL}                   - show vmnode <n> systemd-cgls
                        {Style.NORMAL}-systemd-cgtop   {Style.DIM}[options]{Style.RESET_ALL}                   - show vmnode <n> systemd-cgtop
                        {Style.NORMAL}-systemd-run     {Style.DIM}[options]{Style.RESET_ALL}                   - show vmnode <n> systemd-run
                        {Style.NORMAL}-cgroupControllerUtil     {Style.DIM}[command] [username|uid] [groupname|gid][group path]{Style.RESET_ALL}                   - cgroup controller utilities

    [XcalarService]
    {Style.BRIGHT}remote {Style.DIM}-n <number>  {Style.NORMAL}-xcalar-usrnode.service  {Style.DIM}[start]{Style.RESET_ALL}             - xcalar-usrnode.service on vmnode <n>
                                                 {Style.DIM}[stop]{Style.RESET_ALL}
                                                 {Style.DIM}[restart]{Style.RESET_ALL}
                        {Style.NORMAL}-xccli     {Style.DIM}[options]{Style.RESET_ALL}                         - show vmnode <n> xccli

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

        # remote_nodes = xcalar_properties.nodes
        remote_nodes = XShell_Connector.get_cluster_nodes()
        hostname = remote_nodes.get(n)

        arguments = ' '.join(tokens[3::]) if len(tokens) > 3 else ''
        print(arguments)
        # action
        # remote = "ssh -o LogLevel=QUIET -t"
        remote = 'ssh -o LogLevel=QUIET -t'

        # ============
        # [SysStat]
        # ============
        if len(tokens) == 1 and tokens[0] == '-list':
            for i, name in remote_nodes.items():
                print(f'\t{i}:{name}')

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-free':
            command = f"""{remote} {hostname} 'free {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-htop':
            command = f"""{remote} {hostname} 'htop {arguments}'"""
            os.system(command)

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-iostat':
            command = f"""{remote} {hostname} 'iostat {arguments}'"""
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

        elif len(tokens) >= 3 and tokens[0] == '-n' and tokens[2] == '-perf':
            command = f"""{remote} {hostname} 'sudo perf {arguments}'"""
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
            '-list',
            '-htop',
            '-iostat',
            '-lsof',
            '-n',
            '-mpstat',
            '-perf',
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
            '-xcalar-usrnode.service',
            '-cgroupControllerUtil',
        ]

    ipython.register_magic_function(dispatcher, 'line', magic_name='remote')
    ipython.set_hook('complete_command', auto_completers, str_key='remote')
    ipython.set_hook('complete_command', auto_completers, str_key='%remote')


def unload_ipython_extension(ipython):
    pass
