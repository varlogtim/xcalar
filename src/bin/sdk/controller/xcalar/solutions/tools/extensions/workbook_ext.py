# SF-482
# import json
import pandas as pd

# from xcalar.solutions.singleton import Singleton_Controller
from xcalar.solutions.tools.connector import XShell_Connector
from colorama import Style
from tabulate import tabulate

# from pygments import highlight
# from pygments.lexers import JsonLexer
# from pygments.formatters import TerminalFormatter


def dispatcher(line):
    # {Style.NORMAL}workbook {Style.DIM}-list_apps    <workbook>{Style.RESET_ALL}                                 {Style.BRIGHT}- lists all optimized dataflow names within a workbook.{Style.RESET_ALL}
    help = {
        'show_all':
            f"""
Usage:

WORKBOOK
    {Style.NORMAL}workbook {Style.DIM}-list{Style.RESET_ALL}                                                    {Style.BRIGHT}- lists all workbooks’ names.{Style.RESET_ALL}
    {Style.NORMAL}workbook {Style.DIM}-describe     <workbook>{Style.RESET_ALL}                                 {Style.BRIGHT}- shows name, status of a workbook.{Style.RESET_ALL}
    {Style.NORMAL}workbook {Style.DIM}-list_modules <workbook>{Style.RESET_ALL}                                 {Style.BRIGHT}- lists all dataflow names within a workbook.{Style.RESET_ALL}
    {Style.NORMAL}workbook {Style.DIM}-list_udfs    <workbook>{Style.RESET_ALL}                                 {Style.BRIGHT}- lists all udf names within a workbook.{Style.RESET_ALL}
""",
        'workbook_list':
            f"""
Usage:
    WORKBOOK
    {Style.NORMAL}workbook {Style.DIM}-list{Style.RESET_ALL}                            {Style.BRIGHT}- lists all workbooks’ names.{Style.RESET_ALL}
        """,
        'workbook_describe':
            f"""
Usage:
    WORKBOOK
    {Style.NORMAL}workbook {Style.DIM}-describe <workbook>{Style.RESET_ALL}             {Style.BRIGHT}- shows name, status of a workbook.{Style.RESET_ALL}
        """,
        'workbook_list_modules':
            f"""
Usage:
    WORKBOOK
        {Style.NORMAL}workbook {Style.DIM}-list_modules <workbook>{Style.RESET_ALL}         {Style.BRIGHT}- lists all dataflow names within a workbook.{Style.RESET_ALL}
        """,
        'workbook_apps':
            f"""
Usage:
    WORKBOOK
        {Style.NORMAL}workbook {Style.DIM}-list_apps <workbook>{Style.RESET_ALL}            {Style.BRIGHT}- lists all optimized dataflow names within a workbook.{Style.RESET_ALL}
        """,
        'workbook_udfs':
            f"""
Usage:
    WORKBOOK
        {Style.NORMAL}workbook {Style.DIM}-list_udfs  <workbook>{Style.RESET_ALL}           {Style.BRIGHT}- lists all udf names within a workbook.{Style.RESET_ALL}
        """,
    }

    tokens = line.split()
    # singleton_controller = Singleton_Controller.getInstance()
    # controller = singleton_controller.get_controller()

    if len(tokens) == 0:
        print(help['show_all'])
        return

    try:
        xshell_connector = XShell_Connector()
        controller = xshell_connector.get_controller()
        # workbook
        if tokens[0] == '-list':
            workbooks = controller.orchestrator.dispatcher.xcalarClient.list_workbooks(
            )
            cols = ['name', 'username', 'active']
            data = []
            for workbook in workbooks:
                data.append((workbook.name, workbook.username,
                             workbook.is_active()))
            print(
                tabulate(
                    pd.DataFrame.from_records(data, columns=cols),
                    headers='keys',
                    tablefmt='psql',
                ))

        elif tokens[0] == '-describe':
            if len(tokens) == 2 and tokens[1]:
                workbook = controller.orchestrator.dispatcher.xcalarClient.get_workbook(
                    tokens[1])
                attrs = vars(workbook)
                print(f'{workbook.name}   {attrs}')
            else:
                print(help['workbook_describe'])

        elif tokens[0] == '-list_modules':
            if len(tokens) == 2 and tokens[1]:
                workbook = controller.orchestrator.dispatcher.xcalarClient.get_workbook(
                    tokens[1])
                {print(name) for name in workbook.list_dataflows()}

            else:
                print(help['workbook_list_modules'])

        elif tokens[0] == '-list_apps':
            if len(tokens) == 2 and tokens[1]:
                pass
            else:
                print(help['workbook_list_apps'])

        elif tokens[0] == '-list_udfs':
            if len(tokens) == 2 and tokens[1]:
                workbook = controller.orchestrator.dispatcher.xcalarClient.get_workbook(
                    tokens[1])
                names = list(
                    map(lambda x: x.name, workbook.list_udf_modules('*')))
                {print(name) for name in names}
            else:
                print(help['workbook_list_udfs'])

        else:
            print(help['show_all'])

    except AttributeError as ex:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {ex}')
    except KeyboardInterrupt:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
        return
    except Exception as e:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {e}')


def load_ipython_extension(ipython, *args):
    def workbook_completers(self, event):
        return [
            '-list',
            '-describe',
            '-list_modules',
        # "-list_apps",
            '-list_udfs',
        ]

    ipython.register_magic_function(dispatcher, 'line', magic_name='workbook')
    ipython.set_hook(
        'complete_command', workbook_completers, str_key='workbook')
    ipython.set_hook(
        'complete_command', workbook_completers, str_key='%workbook')


def unload_ipython_extension(ipython):
    pass
