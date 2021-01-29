import xcalar

# from xcalar.solutions.singleton import Singleton_Controller
from xcalar.solutions.tools.connector import XShell_Connector
from colorama import Style


def dispatcher(line):

    usage = """
Usage:
    DATAFLOW
        show dataflows <workbook_name> - lists all dataflows in a workbook workbooks.
        dataflow validate <workbook> <dataflow> - validates a dataflow from a given workbook

    """

    tokens = line.split(' ', 2)
    try:
        # singleton_controller = Singleton_Controller.getInstance()
        # controller = singleton_controller.get_controller()

        xshell_connector = XShell_Connector()
        controller = xshell_connector.get_controller()
        # dataflow
        if len(tokens) == 3 and tokens[0] == 'validate':
            workbook = tokens[1]
            dataflow = tokens[2]

            (successful,
             errors) = controller.orchestrator.dispatcher.validate_dataflow(
                 dataflow, workbook_name=workbook)
            if not successful:
                print('Dataflow is invalid.')
                for title, error in errors.items():
                    print(f'{title}: {error}')

        else:
            print(usage)
    except xcalar.external.workbook.WorkbookStateException as ex:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {ex}')
    except KeyboardInterrupt:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
        return
    except Exception as e:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {e}')


def load_ipython_extension(ipython, *args):
    # use ipython.register_magic_function
    #   to register "dispatcher" function
    #   make magic_name = 'http'
    ipython.register_magic_function(dispatcher, 'line', magic_name='dataflow')


def unload_ipython_extension(ipython):
    pass
