import re
# import pandas as pd
import requests
from xcalar.solutions.tools.connector import XShell_Connector
from tabulate import tabulate
from colorama import Style

# from .action_confirm import confirm


def sql_line_cell(line, cell=None):

    usage = f"""
Usage:
    SQL
    {Style.NORMAL}sql   {Style.DIM}<SQL_statemnet>{Style.RESET_ALL}                                             {Style.BRIGHT}- executes sql and displays first 1000 rows of the result.{Style.RESET_ALL}

    """
    sql_string = None

    if cell is None:
        sql_string = line
    else:
        sql_string = line + ' ' + ' '.join(cell.split('\n'))

    try:
        # ---------------------
        # connectivity check
        # ---------------------
        if not (XShell_Connector.self_check_client()
                and XShell_Connector.self_check_controller()):
            return None

        orchestrator = XShell_Connector.get_orchestrator(
            XShell_Connector.get_universe_id())
        analyzer = orchestrator.getAnalyzer()

        # sql
        if sql_string:
            n = None
            limit_string = re.findall(r' limit\s+\d+', sql_string,
                                      re.IGNORECASE)
            if limit_string:
                n = int(''.join(
                    re.findall(r'\d+', ''.join(limit_string), re.IGNORECASE)))
            # execute
            if n:
                recorders = analyzer.executeSql(sql_string, n=n)
            else:
                recorders = analyzer.executeSql(sql_string)
            print(tabulate(
                recorders,
                headers='keys',
                tablefmt='psql',
            ))

        else:
            print(usage)

    except (AttributeError) as ex:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {ex}')

    except requests.exceptions.HTTPError as err:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {err}')

    except KeyboardInterrupt:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
        return
    except Exception as e:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {e}')


def load_ipython_extension(ipython, *args):

    ipython.register_magic_function(sql_line_cell, 'line', magic_name='sql')
    ipython.register_magic_function(sql_line_cell, 'cell', magic_name='sql')


def unload_ipython_extension(ipython):
    pass
