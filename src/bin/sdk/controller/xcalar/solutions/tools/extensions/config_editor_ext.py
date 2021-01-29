import os
from colorama import Style

from xcalar.solutions.tools.utilities.nodes import get_nodes_list
from xcalar.solutions.tools.utilities.authorized_key_generator import create_authorized_key
from xcalar.solutions.tools.utilities.PlaybookExecutor import XcalarPlaybookExecutor


def dispatcher(line, cell=None):

    usage = f"""
Usage:

Configuration
    {Style.NORMAL}%%config_editor  {Style.DIM}-v <path of file>{Style.RESET_ALL}                                            {Style.BRIGHT}- view configuration files on all nodes.{Style.RESET_ALL}
    {Style.NORMAL}%%config_editor  {Style.DIM}-v <path of file> -i <host1,host2,> {Style.RESET_ALL}                         {Style.BRIGHT}- view configuration files on spedific nodes.{Style.RESET_ALL}
    {Style.NORMAL}%%config_editor  {Style.DIM}-e <path of file>{Style.RESET_ALL}                                            {Style.BRIGHT}- edit configuration files on all nodes.{Style.RESET_ALL}
    {Style.NORMAL}%%config_editor  {Style.DIM}-e <path of file> -i <host1,host2,> {Style.RESET_ALL}                         {Style.BRIGHT}- edit configuration files on spedific nodes.{Style.RESET_ALL}
    {Style.NORMAL}           ...:  {Style.DIM}key1=value1 {Style.RESET_ALL}                     {Style.BRIGHT}              {Style.RESET_ALL}
    {Style.NORMAL}           ...:  {Style.DIM}key2=value2 {Style.RESET_ALL}                     {Style.BRIGHT}              {Style.RESET_ALL}
    {Style.NORMAL}           ...:  {Style.DIM}key3=value3 {Style.RESET_ALL}                     {Style.BRIGHT}              {Style.RESET_ALL}

    """
    if cell is None:
        kv_string = line
    else:
        kv_string = ' '.join(cell.split('\n'))

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

        if cell is None:
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
        os.environ['ANSIBLE_STDOUT_CALLBACK'] = 'yaml'
        os.environ['ANSIBLE_LOAD_CALLBACK_PLUGINS'] = 'true'
        os.environ['ANSIBLE_LOAD_CALLBACKs'] = 'true'
        os.environ['ANSIBLE_STRATEGY'] = 'linear'

        create_authorized_key()

        # ---------------------
        # options check
        # ---------------------
        cell_string = ' '.join(cell.split('\n'))

        if len(tokens) == 2 and tokens[0] == '-v':
            file_path = tokens[1]
            play_source = dict(
                name=f'view {file_path}',
                hosts='all',
                tasks=[
                    dict(
                        action=dict(module='command', args=f'cat {file_path}'),
                        register='shell_out'),
                    dict(
                        action=dict(
                            module='debug',
                            args=dict(msg='{{shell_out.stdout_lines}}')))
                ])

        elif len(tokens) == 4 and tokens[0] == '-v' and tokens[2] == '-i':
            file_path = tokens[1]
            hosts_string = tokens[3]

            play_source = dict(
                name=f'view {file_path}',
                hosts='all',
                tasks=[
                    dict(
                        action=dict(module='command', args=f'cat {file_path}'),
                        register='shell_out'),
                    dict(
                        action=dict(
                            module='debug',
                            args=dict(msg='{{shell_out.stdout_lines}}')))
                ])

        elif len(tokens) == 2 and tokens[0] == '-e':
            file_path = tokens[1]
            pairs = cell.split('\n')
            kv_pairs = []
            for kv in pairs:
                print(kv)

            for kv_string in pairs:
                if '=' in kv_string:
                    data = kv_string.strip().split('=')
                    key = data[0]
                    value = data[1]
                    tmp = dict(section='', option=f'{key}', value=f'{value}')
                    kv_pairs.append(tmp)

            play_source = {
                'hosts':
                    'all',
                'remote_user':
                    'xcalar',
                'tasks': [{
                    'name': f'file: {file_path}',
                    'ini_file': {
                        'path': f'{file_path}',
                        'mode': '0644',
                        'backup': True,
                        'section': '{{ item.section }}',
                        'option': '{{ item.option }}',
                        'value': '{{ item.value }}'
                    },
                    'with_items': kv_pairs
                }]
            }

        elif len(tokens) == 4 and tokens[0] == '-e' and tokens[2] == '-i':
            file_path = tokens[1]
            hosts_string = tokens[3]

            pairs = cell.split('\n')
            kv_pairs = []
            for kv in pairs:
                print(kv)

            for kv_string in pairs:
                if '=' in kv_string:
                    data = kv_string.strip().split('=')
                    key = data[0]
                    value = data[1]
                    tmp = dict(section='', option=f'{key}', value=f'{value}')
                    kv_pairs.append(tmp)

            play_source = {
                'hosts':
                    'all',
                'remote_user':
                    'xcalar',
                'tasks': [{
                    'name': f'file: {file_path}',
                    'ini_file': {
                        'path': f'{file_path}',
                        'mode': '0644',
                        'backup': True,
                        'section': '{{ item.section }}',
                        'option': '{{ item.option }}',
                        'value': '{{ item.value }}'
                    },
                    'with_items': kv_pairs
                }]
            }

        else:
            print(usage)
            return

        # ---------------------
        # executor
        # ---------------------
        print(hosts_string)
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
        return [
            '-i',
            '-f',
        ]

    ipython.register_magic_function(
        dispatcher, 'cell', magic_name='config_editor')
    # ipython.register_magic_function(dispatcher, 'line', magic_name='config_editor')
    # ipython.set_hook('complete_command', auto_completers, str_key='config_editor')
    ipython.set_hook(
        'complete_command', auto_completers, str_key='%config_editor')


def unload_ipython_extension(ipython):
    pass
