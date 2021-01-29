# from xcalar.solutions.singleton import Singleton_Controller
from xcalar.solutions.tools.connector import XShell_Connector
from colorama import Style


def dispatcher(line):

    usage = """
Usage:
    session - show current session name.

    """

    tokens = line.split()

    try:
        # ---------------------
        # connectivity check
        # ---------------------
        if not (XShell_Connector.self_check_client()
                and XShell_Connector.self_check_controller()):
            return None

        session = XShell_Connector.get_session_obj()
        if len(tokens) == 0:
            print(session.name)

        else:
            print(usage)
    except AttributeError as ex:
        print(f'{ex}, please run "materialize" first')
    except KeyboardInterrupt:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
        return
    except Exception as e:
        print(e)


def load_ipython_extension(ipython, *args):
    # use ipython.register_magic_function
    #   to register "dispatcher" function
    #   make magic_name = 'http'
    ipython.register_magic_function(dispatcher, 'line', magic_name='session')


def unload_ipython_extension(ipython):
    pass
