# import json
# import pandas as pd
from xcalar.solutions.tools.connector import XShell_Connector

# from xcalar.solutions.singleton import Singleton_Controller
from colorama import Style
# from tabulate import tabulate
# from pygments import highlight
# from pygments.lexers import JsonLexer
# from pygments.formatters import TerminalFormatter
from xcalar.solutions.git.git_api import GitApi
from xcalar.solutions.sdlc import SdlcManager
from .action_confirm import confirm


def dispatcher(line):
    help = {
        'show_all':
            f"""
Usage:

UDF
    {Style.NORMAL}udf {Style.DIM}-list{Style.RESET_ALL}                                                         {Style.BRIGHT}- lists all udfs, including name, workbook_name, shared status.{Style.RESET_ALL}
    {Style.NORMAL}udf {Style.DIM}-add           <workbook> <path>{Style.RESET_ALL}                              {Style.BRIGHT}- adds a udf from vm to a workbook.{Style.RESET_ALL}
    {Style.NORMAL}udf {Style.DIM}-checkin       <workbook> <udf> <git_branch> <git_path>{Style.RESET_ALL}       {Style.BRIGHT}- checkins given udf(s) from a workbook in a git branch and path.{Style.RESET_ALL}
    {Style.NORMAL}udf {Style.DIM}-checkout      <udf> <git_branch> <git_path>{Style.RESET_ALL}                  {Style.BRIGHT}- checkouts given udf(s) into a workbook from a git branch and path.{Style.RESET_ALL}
""",
        'udf_list':
            f"""
Usage:
    UDF
    {Style.NORMAL}udf {Style.DIM}-list{Style.RESET_ALL}                           {Style.BRIGHT}- lists all udfs, including name, workbook_name, shared status.{Style.RESET_ALL}
        """,
        'udf_add':
            f"""
Usage:
    UDF
    {Style.NORMAL}udf {Style.DIM}-add <workbook> <path>{Style.RESET_ALL}          {Style.BRIGHT}- adds a udf from vm to a workbook.{Style.RESET_ALL}
        """,
        'udf_checkin':
            f"""
Usage:
    UDF
    {Style.NORMAL}udf {Style.DIM}-checkin <workbook> <udf> <git_branch> <git_path>{Style.RESET_ALL}         {Style.BRIGHT}- checkins given udf(s) from a workbook in a git branch and path.{Style.RESET_ALL}
        """,
        'udf_checkout':
            f"""
Usage:
    UDF
    {Style.NORMAL}udf {Style.DIM}-checkout  <udf> <git_branch> <git_path>{Style.RESET_ALL}                  {Style.BRIGHT}- checkouts given udf(s) into a workbook from a git branch and path.{Style.RESET_ALL}
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
        # udf
        if tokens[0] == '-list':
            pass

        elif tokens[0] == '-add':
            if len(tokens) == 3 and tokens[1] and tokens[2]:
                if confirm() is None:
                    return
                pass
            else:
                print(help['udf_add'])

        elif tokens[0] == '-drop':
            if len(tokens) == 2 and tokens[1]:
                if confirm() is None:
                    return
                pass
            else:
                print(help['udf_drop'])

        elif tokens[0] == '-checkin':
            if len(tokens) == 5 and tokens[1] and tokens[2] and tokens[
                    3] and tokens[4]:
                if confirm() is None:
                    return
                workbook_name = tokens[1]
                udf_name = tokens[2]
                branch_name = tokens[3]
                path = tokens[4]

                controller.orchestrator.dispatcher.checkin_udfs(
                    udf_name, branch_name, path, workbook_name=workbook_name)

                # need to read from properties file
                url = 'https://gitlab.com/api/v4'
                project_id = '14164315'
                access_token = 'Tavay5JsBjYzTq25DyXV'

                gitapi = GitApi(url, project_id, access_token)
                sdlc = SdlcManager(controller.orchestrator.dispatcher, gitapi)

                sdlc.checkin_udfs(
                    udf_name, branch_name, path, workbook_name=workbook_name)
            else:
                print(help['udf_checkin'])

        elif tokens[0] == '-checkout':
            if len(tokens) == 4 and tokens[1] and tokens[2] and tokens[3]:
                if confirm() is None:
                    return
                udf_name = tokens[1]
                branch_name = tokens[2]
                path = tokens[3]

                controller.orchestrator.dispatcher.checkout_udf(
                    udf_name, branch_name, path)

                # need to read from properties file
                url = 'https://gitlab.com/api/v4'
                project_id = '14164315'
                access_token = 'Tavay5JsBjYzTq25DyXV'

                gitapi = GitApi(url, project_id, access_token)
                sdlc = SdlcManager(controller.orchestrator.dispatcher, gitapi)

                sdlc.checkout_udf(udf_name, branch_name, path)
            else:
                print(help['udf_checkout'])

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
    def udf_completers(self, event):
        return [
            '-list',
            '-add',
            '-drop',
            '-checkin',
            '-checkout',
        ]

    ipython.register_magic_function(dispatcher, 'line', magic_name='udf')
    ipython.set_hook('complete_command', udf_completers, str_key='udf')
    ipython.set_hook('complete_command', udf_completers, str_key='%udf')


def unload_ipython_extension(ipython):
    pass
