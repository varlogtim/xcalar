import os
from colorama import Style
from xcalar.solutions.tools.connector import XShell_Connector, xshell_conf
from structlog import get_logger
logger = get_logger(__name__)


def dispatcher(line):

    help = {
        'show_all':
            f"""
Usage:

autoconfirm
    {Style.NORMAL}auto_confirm {Style.DIM}{Style.RESET_ALL}                    {Style.BRIGHT}- by pass confirm[y/N]{Style.RESET_ALL}

""",
    }

    tokens = line.split()

    try:

        if len(tokens) == 1:
            xcautoconfirm = tokens[0]
            xcautoconfirm.replace('"', '')
            xcautoconfirm.replace("'", '')
            xcautoconfirm = f'{xcautoconfirm}'
            upd_objs = {'xcautoconfirm': xcautoconfirm}
            XShell_Connector.update(xshell_conf, upd_objs)
            os.environ['XCAUTOCONFIRM'] = xcautoconfirm

        else:
            print(help['show_all'])

    except AttributeError as ex:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {ex}')
    except Exception as e:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {e}')
    except KeyboardInterrupt:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
        return


def load_ipython_extension(ipython, *args):
    def auto_completers(self, event):
        return ['yes', 'no']

    ipython.register_magic_function(
        dispatcher, 'line', magic_name='auto_confirm')
    ipython.set_hook(
        'complete_command', auto_completers, str_key='auto_confirm')
    ipython.set_hook(
        'complete_command', auto_completers, str_key='%auto_confirm')


def unload_ipython_extension(ipython):
    pass
