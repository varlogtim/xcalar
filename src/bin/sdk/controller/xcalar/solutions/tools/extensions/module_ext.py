# import json
# import pandas as pd

# from xcalar.solutions.singleton import Singleton_Controller
# from xcalar.solutions.tools.connector import XShell_Connector
from colorama import Style
# from tabulate import tabulate
# from pygments import highlight
# from pygments.lexers import JsonLexer
# from pygments.formatters import TerminalFormatter
from .action_confirm import confirm


def dispatcher(line):
    help = {
        'show_all':
            f"""
Usage:

MODULE
    {Style.NORMAL}module {Style.DIM}-list{Style.RESET_ALL}                                                      {Style.BRIGHT}- lists all modules in a workbook.{Style.RESET_ALL}
    {Style.NORMAL}module {Style.DIM}-run        <workbook> <module>{Style.RESET_ALL}                            {Style.BRIGHT}- gets the query of an app.{Style.RESET_ALL}
    {Style.NORMAL}module {Style.DIM}-query      <workbook> <module>{Style.RESET_ALL}                            {Style.BRIGHT}- gets the query of an app.{Style.RESET_ALL}
    {Style.NORMAL}module {Style.DIM}-validate   <workbook> <module> {Style.RESET_ALL}                           {Style.BRIGHT}- validates a module from a given workbook.{Style.RESET_ALL}
    {Style.NORMAL}module {Style.DIM}-checkin    <workbook> <module> <git_branch> <git_path>{Style.RESET_ALL}    {Style.BRIGHT}- checkins given module(s) from a workbook in a git branch and path.{Style.RESET_ALL}
    {Style.NORMAL}module {Style.DIM}-checkout   <workbook> <module> <git_branch> <git_path>{Style.RESET_ALL}    {Style.BRIGHT}- checkouts given module(s) into a workbook from a git branch and path.{Style.RESET_ALL}
""",
        'module_list':
            f"""
Usage:
    MODULE
    {Style.NORMAL}module {Style.DIM}-list{Style.RESET_ALL}                                  {Style.BRIGHT}- lists all modules in a workbook.{Style.RESET_ALL}
        """,
        'module_run':
            f"""
Usage:
    MODULE
    {Style.NORMAL}module {Style.DIM}-run <workbook> <module>{Style.RESET_ALL}               {Style.BRIGHT}- gets the query of an app.{Style.RESET_ALL}
        """,
        'module_query':
            f"""
Usage:
    MODULE
    {Style.NORMAL}module {Style.DIM}-query <workbook> <module>{Style.RESET_ALL}             {Style.BRIGHT}- gets the query of an app.{Style.RESET_ALL}
        """,
        'module_validate':
            f"""
Usage:
    MODULE
    {Style.NORMAL}module {Style.DIM}-validate <workbook> <module> {Style.RESET_ALL}         {Style.BRIGHT}- validates a module from a given workbook.{Style.RESET_ALL}
        """,
        'module_checkin':
            f"""
Usage:
    MODULE
    {Style.NORMAL}module {Style.DIM}-checkin <workbook> <module> <git_branch> <git_path>{Style.RESET_ALL}   {Style.BRIGHT}- checkins given module(s) from a workbook in a git branch and path.{Style.RESET_ALL}
        """,
        'module_checkout':
            f"""
Usage:
    MODULE
    {Style.NORMAL}module {Style.DIM}-checkout <workbook> <module> <git_branch> <git_path>{Style.RESET_ALL}  {Style.BRIGHT}- checkouts given module(s) into a workbook from a git branch and path.{Style.RESET_ALL}
        """,
    }

    tokens = line.split()

    if len(tokens) == 0:
        print(help['show_all'])
        return

    try:
        # singleton_controller = Singleton_Controller.getInstance()
        # controller = singleton_controller.get_controller()
        # xshell_connector = XShell_Connector()
        # controller = xshell_connector.get_controller()

        # module
        if tokens[0] == '-list':
            if len(tokens) == 2 and tokens[1]:
                pass
            else:
                print(help['module_list'])

        elif tokens[0] == '-run':
            if len(tokens) == 3 and tokens[1] and tokens[2]:
                pass
            else:
                print(help['module_run'])

        elif tokens[0] == '-query':
            if len(tokens) == 2 and tokens[1]:
                pass
            else:
                print(help['module_query'])

        elif tokens[0] == '-validate':
            if len(tokens) == 2 and tokens[1]:
                pass
            else:
                print(help['module_validate'])

        elif tokens[0] == '-checkin':
            if len(tokens) == 5 and tokens[1] and tokens[2] and tokens[
                    3] and tokens[4]:
                if confirm() is None:
                    return
                # workbook_name = tokens[1]
                # module_name = tokens[2]
                # git_branch = tokens[3]
                # git_path = tokens[4]
                pass
            else:
                print(help['module_checkin'])

        elif tokens[0] == '-checkout':
            if len(tokens) == 5 and tokens[1] and tokens[2] and tokens[
                    3] and tokens[4]:
                if confirm() is None:
                    return
                # workbook_name = tokens[1]
                # module_name = tokens[2]
                # git_branch = tokens[3]
                # git_path = tokens[4]
                pass
            else:
                print(help['module_checkout'])

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
    def module_completers(self, event):
        return [
            '-list',
            '-run',
            '-query',
            '-validate',
            '-checkin',
            '-checkout',
        ]

    ipython.register_magic_function(dispatcher, 'line', magic_name='module')
    ipython.set_hook('complete_command', module_completers, str_key='module')
    ipython.set_hook('complete_command', module_completers, str_key='%module')


def unload_ipython_extension(ipython):
    pass
