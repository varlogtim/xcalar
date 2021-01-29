import os
from colorama import Style

from xcalar.solutions.tools.utilities.nodes import get_nodes_list
from xcalar.solutions.tools.utilities.authorized_key_generator import create_authorized_key
from xcalar.solutions.tools.utilities.PlaybookExecutor import XcalarPlaybookExecutor


def dispatcher(line):

    usage = f"""
Usage:

Manage Services
    {Style.NORMAL}systemctl {Style.DIM}reload       <service>{Style.RESET_ALL}                     {Style.BRIGHT}- reload service.{Style.RESET_ALL}
    {Style.NORMAL}systemctl {Style.DIM}restart      <service>{Style.RESET_ALL}                     {Style.BRIGHT}- restart service.{Style.RESET_ALL}
    {Style.NORMAL}systemctl {Style.DIM}start        <service>{Style.RESET_ALL}                     {Style.BRIGHT}- start service.{Style.RESET_ALL}
    {Style.NORMAL}systemctl {Style.DIM}stop         <service>{Style.RESET_ALL}                     {Style.BRIGHT}- stop service.{Style.RESET_ALL}
    {Style.NORMAL}systemctl {Style.DIM}status       <service>{Style.RESET_ALL}                     {Style.BRIGHT}- show status of service.{Style.RESET_ALL}
    {Style.NORMAL}systemctl {Style.DIM}list-units            {Style.RESET_ALL}                     {Style.BRIGHT}- list all units.{Style.RESET_ALL}

    """

    tokens = line.split()
    hosts_list = get_nodes_list()
    if not hosts_list:
        hosts_list = ['127.0.0.1']
    hosts_string = ','.join(hosts_list)

    try:

        # ---------------------
        # show menu
        # ---------------------
        if len(tokens) == 0:
            print(usage)
            return

        # ---------------------
        # authorized key check
        # ---------------------
        os.environ['ANSIBLE_PYTHON_INTERPRETER'] = 'auto_silent'
        os.environ['ANSIBLE_HOST_KEY_CHECKING'] = 'false'
        os.environ['ANSIBLE_FORCE_COLOR'] = 'true'
        os.environ['ANSIBLE_ACTION_WARNINGS'] = 'false'
        os.environ['ANSIBLE_COMMAND_WARNINGS'] = 'false'
        os.environ['ANSIBLE_BECOME_ASK_PASS'] = 'false'

        os.environ['ANSIBLE_LOCAL_TEMP'] = '/tmp/.ansible/tmp'
        os.environ['ANSIBLE_PERSISTENT_CONTROL_PATH_DIR'] = '/tmp/.ansible/cp'
        os.environ['ANSIBLE_REMOTE_TEMP'] = '/tmp/.ansible/tmp'
        os.environ['ANSIBLE_SSH_CONTROL_PATH_DIR'] = '/tmp/.ansible/cp'
        create_authorized_key()

        # ---------------------
        # options check
        # ---------------------
        if tokens[0] == 'reload':
            service_name = tokens[1]
            if len(tokens) == 4 and tokens[2] == '-i':
                hosts_string = tokens[3]

            play_source = dict(
                name=f'reload {service_name}',
                hosts='all',
                tasks=[
                    dict(
                        action=dict(
                            module='systemd',
                            args=dict(
                                name=f'{service_name}', state='reloaded')))
                ])

        elif tokens[0] == 'restart':
            service_name = tokens[1]
            if len(tokens) == 4 and tokens[2] == '-i':
                hosts_string = tokens[3]

            play_source = dict(
                name=f'restart {service_name}',
                hosts='all',
                tasks=[
                    dict(
                        action=dict(
                            module='systemd',
                            args=dict(
                                name=f'{service_name}',
                                state='restarted',
                                daemon_reload='yes')))
                ])

        elif tokens[0] == 'start':
            service_name = tokens[1]
            if len(tokens) == 4 and tokens[2] == '-i':
                hosts_string = tokens[3]

            play_source = dict(
                name=f'start {service_name}',
                hosts='all',
                tasks=[
                    dict(
                        action=dict(
                            module='systemd',
                            args=dict(name=f'{service_name}',
                                      state='started')))
                ])

        elif tokens[0] == 'stop':
            service_name = tokens[1]
            if len(tokens) == 4 and tokens[2] == '-i':
                hosts_string = tokens[3]

            play_source = dict(
                name=f'stop {service_name}',
                hosts='all',
                environment=[dict(ANSIBLE_BECOME_ASK_PASS='false')],
                tasks=[
                    dict(
                        action=dict(
                            module='systemd',
                            args=dict(name=f'{service_name}',
                                      state='stopped')))
                ])

        elif tokens[0] == 'status':
            service_name = tokens[1]
            if len(tokens) == 4 and tokens[2] == '-i':
                hosts_string = tokens[3]

            play_source = dict(
                name=f'status {service_name}',
                hosts='all',
                tasks=[
                    dict(
                        action=dict(
                            module='command',
                            args=f'service {service_name} status'),
                        register='shell_out'),
                    dict(
                        action=dict(
                            module='debug',
                            args=dict(msg='{{shell_out.stdout_lines}}')))
                ])

        elif tokens[0] == 'list-units':
            hostname = hosts_list[0]
            remote = 'ssh -o LogLevel=QUIET -t'
            command = f'{remote} {hostname} systemctl list-units'
            os.system(command)
            return

        else:
            print(usage)
            return

        # ---------------------
        # executor
        # ---------------------
        executor = XcalarPlaybookExecutor(
            play_source=play_source,
            remote_user='xcalar',
            private_key_file='/home/xcalar/.ssh/id_rsa',
            inventory_file_list=f'{hosts_string},')
        executor.run()

    except Exception as e:
        print(e)


def load_ipython_extension(ipython, *args):
    def auto_completers(self, event):
        return ['reload', 'restart', 'start', 'stop', 'status', 'list-units']

    ipython.register_magic_function(dispatcher, 'line', magic_name='systemctl')
    ipython.set_hook('complete_command', auto_completers, str_key='systemctl')
    ipython.set_hook('complete_command', auto_completers, str_key='%systemctl')


def unload_ipython_extension(ipython):
    pass
