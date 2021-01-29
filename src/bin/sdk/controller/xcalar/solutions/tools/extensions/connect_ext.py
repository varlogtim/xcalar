import getpass
import os
from colorama import Style
from xcalar.solutions.tools.connector import XShell_Connector


def dispatcher(line):

    usage = f"""
Usage:
    {Style.NORMAL}connect {Style.DIM}-url <url> -u <username> -p{Style.RESET_ALL}                                                    {Style.BRIGHT}- connect to clusters{Style.RESET_ALL}

    """

    tokens = line.split()
    xshell_connector = XShell_Connector()

    if (len(tokens) == 5 and tokens[0] == '-url' and tokens[2] == '-u'
            and tokens[4] == '-p'):
        url = tokens[1]
        username = tokens[3]
        password = getpass.getpass('Password : ')
        try:
            xshell_connector.update_xshell_client(url, username, password)

        except AttributeError as ex:
            print(f'{Style.DIM}Warning: {Style.RESET_ALL} {ex}')
        except Exception as e:
            print(f'{Style.DIM}Warning: {Style.RESET_ALL} {e}')
        except KeyboardInterrupt:
            print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
            return

    # -------------------------
    # for Lin can pass passowrd
    # -------------------------
    elif (len(tokens) == 6 and tokens[0] == '-url' and tokens[2] == '-u'
          and tokens[4] == '-p'):
        url = tokens[1]
        username = tokens[3]
        password = tokens[5]
        try:
            xshell_connector.update_xshell_client(url, username, password)

        except AttributeError as ex:
            print(f'{Style.DIM}Warning: {Style.RESET_ALL} {ex}')
        except Exception as e:
            print(f'{Style.DIM}Warning: {Style.RESET_ALL} {e}')
        except KeyboardInterrupt:
            print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
            return

    elif len(tokens) == 1 and tokens[0] is not None:
        os.environ['XCALAR_UNIVERSE_ID'] = tokens[0]
        print(f"switch to {os.getenv('XCALAR_UNIVERSE_ID')}")

    else:
        print(usage)


def load_ipython_extension(ipython, *args):
    def auto_completers(self, event):
        return [
            '-url',
            '-u',
            '-p',
        ]

    ipython.register_magic_function(dispatcher, 'line', magic_name='connect')
    ipython.set_hook('complete_command', auto_completers, str_key='connect')
    ipython.set_hook('complete_command', auto_completers, str_key='%connect')


def unload_ipython_extension(ipython):
    pass
