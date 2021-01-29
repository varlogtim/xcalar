import os
from colorama import Style

from xcalar.solutions.tools.utilities.nodes import get_nodes_from_cfg


def tmux_cmd(remote_nodes):
    # ============
    # [split 2]
    # ============
    nodes = []
    for idx, hostname in remote_nodes.items():
        nodes.append(hostname)

    if len(nodes) == 2:
        cmd = f"""#!/bin/bash
            tmux new-session -d 'ssh  {nodes[0]}'
            tmux split-window -v 'ssh {nodes[1]}'
            tmux -2 attach-session -d
        """
        return cmd

    elif len(nodes) == 3:
        cmd = f"""#!/bin/bash
            tmux new-session -d 'ssh  {nodes[0]}'
            tmux split-window -v 'ssh {nodes[1]}'
            tmux split-window -h 'ssh {nodes[2]}'
            tmux -2 attach-session -d
        """
        return cmd

    else:
        # ============
        # [all in one]
        # ============
        nodes = []
        for idx, hostname in remote_nodes.items():
            nodes.append(
                f"tmux new-window -t sites:{idx} -n 'vm{idx}' 'ssh {hostname}'"
            )

        windows = '\n'.join(map(str, nodes))

        cmd = f"""#!/bin/bash
            tmux new-session -d -s sites
            {windows}
            tmux select-window -t sites:1
            tmux -2 attach-session -t sites
        """
        return cmd


def dispatcher(line):

    usage = f"""
Usage:

    {Style.BRIGHT}tmux {Style.NORMAL}-all{Style.DIM}{Style.RESET_ALL}                                                   - it enables all vm's terminals to be created, accessed, and controlled from a single screen.

    """

    tokens = line.split()

    # ---------------------
    # connectivity check
    # ---------------------
    # if not XShell_Connector.self_check_client():
    #    return None

    # singleton_controller = Singleton_Controller.getInstance()
    # controller = singleton_controller.get_controller()

    try:
        remote_nodes = get_nodes_from_cfg()
        # ============
        # [tmux]
        # ============
        if len(tokens) == 1 and tokens[0] == '-all':
            os.system(tmux_cmd(remote_nodes))

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
            '-all',
        ]

    ipython.register_magic_function(dispatcher, 'line', magic_name='tmux')
    ipython.set_hook('complete_command', auto_completers, str_key='tmux')
    ipython.set_hook('complete_command', auto_completers, str_key='%tmux')


def unload_ipython_extension(ipython, extension_name):
    del ipython.magics_manager.magics['line'][extension_name]
    pass
