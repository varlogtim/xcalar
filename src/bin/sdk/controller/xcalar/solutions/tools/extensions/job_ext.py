import json
import re
import pandas as pd
import pprint
import traceback
import multiprocessing

from xcalar.solutions.tools.connector import XShell_Connector, xshell_conf
from xcalar.compute.util.xcalar_top_util import xcalar_top_main
from xcalar.compute.util.xcalar_top_util import get_program_description
from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.solutions.analyzer import Analyzer
from tabulate import tabulate
from colorama import Style, Fore
from .action_confirm import confirm

pp = pprint.PrettyPrinter(indent=1, width=80, compact=False)
default_refresh_interval = 5
default_num_parallel_procs = multiprocessing.cpu_count()
default_iters = 100

'''
Fore: BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE, RESET.
Back: BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE, RESET.
Style: DIM, NORMAL, BRIGHT, RESET_ALL
'''


def dispatcher(line):
    help = {
        'all_usage': f"""
Usage:

JOB
    {Style.NORMAL}job {Style.DIM}-list <state>{Style.RESET_ALL}                                                 {Style.BRIGHT}- lists jobs by state name, use '-list' to show all.{Style.RESET_ALL}
    {Style.NORMAL}job {Style.DIM}-cancel <job_name>{Style.RESET_ALL}                                            {Style.BRIGHT}- cancel running jobs.{Style.RESET_ALL}
    {Style.NORMAL}job {Style.DIM}-top --interval <interval> --num_procs <num_procs> --verbose --iters <iterations>{Style.RESET_ALL}  {Style.BRIGHT}- show live jobs stats.
                                                                        - interval - {Style.DIM}Optional. Defaults to 5 seconds.{Style.RESET_ALL}
                                                                        - {Style.BRIGHT}num_procs - {Style.DIM}Optional. Defaults to num cores.{Style.RESET_ALL}
                                                                        - {Style.BRIGHT}iters - {Style.DIM}Optional. Number of iterations. Defaults to 100.{Style.RESET_ALL}
                                                                        - {Style.BRIGHT}verbose - {Style.DIM}Optional flag. If set, full job_id will be displayed. {Style.RESET_ALL}

"job -top" Description:
    This utility reports live stats for jobs executing in optimized mode.

    For each job, the following are reported (column headings are on left):

        JOB:                    Name of job (truncated after a limit)
        SESSION:                Session-name or ID, depending on how job was invoked
        STATE:                  One of [Runnable, Running, Success, Failed, Cancelled]
        ELAPSED(s):             Elapsed time in seconds
        %PROGRESS (w/c/f):      % of graph-nodes completed, no. waiting/completed/failed
        OPERATOR (%completed):  Current running operator (% completed).
        W0RKING SET:            Current working set size of job. Takes into account the fact
                                that tables aren't dropped until all child graph nodes are done
                                using it.
        SKEW:                   Load imbalance across cluster nodes.
                                Values are from 0 (perfectly balanced) to 100 (most imbalanced).
                                Skew is across the job (including operators executed so far).
        SKEWTIME%:              % of elapsed time spent in high skew (> 80) operators.
        NETW:                   Transport page (bytes) sent between cluster-nodes.
        SIZE:                   Size (bytes) of all tables ever created by the job.
                                NOTE: some of these tables may have been dropped.
        PGDENSE%:               % page density (Xcalar buffer cache page utilization)

    The tool runs like 'htop' and will quit when the 'q' key is entered. If this
    doesn't work, use "ctrl-C" to quit.

    Entering one of the following keys will take the specified action

    "e": sort (descending) by ELAPSED(s) (this is default sort key)
    "s": sort (descending) by SIZE
    "d": dump a JSON snapshot of current top output into xcalar_top_out.json

    For a seamless experience, the tool must run in a window with at least 170
    columns and a height which accommodates all jobs in the system. If not, the tool
    may error out with a failure, asking the user to resize the window
"""}

    # -----------------------
    # get user input
    # -----------------------
    tokens = line.split()

    if len(tokens) == 0:
        print(help['all_usage'])
        return

    try:
        # ---------------------
        # connectivity check
        # ---------------------
        if not (
            XShell_Connector.self_check_client()
            and XShell_Connector.self_check_controller()
        ):
            return None

        client = XShell_Connector.get_client()

        qstateToStr = {
            'qrNotStarted': 'Runnable',
            'qrProcessing': 'Running',
            'qrFinished': 'Success',
            'qrError': 'Failed',
            'qrCancelled': 'Cancelled',
        }

        if tokens[0] == '-list':
            if len(tokens) == 2:
                matched_state = tokens[1].capitalize()
                xcalarapi = XShell_Connector.get_xcalarapi()
                queries = xcalarapi.listQueries('*').queries
                # print(queries)

                name_list = []
                elapsed_list = []
                state_list = []
                for q in queries:
                    state = qstateToStr[q.state]
                    if state == matched_state and matched_state in [
                        'Runnable',
                        'Running',
                        'Success',
                        'Failed',
                        'Cancelled',
                    ]:
                        elapsed = q.milliseconds_elapsed
                        name_list.append(q.name)
                        elapsed_list.append(elapsed)
                        state_list.append(state)
                    else:
                        pass

                table = zip(name_list, elapsed_list, state_list)

                print(
                    tabulate(
                        table,
                        headers=['job name', 'elapsed (milliseconds)', 'state'],
                        tablefmt='psql',
                    ),
                )

            else:
                xcalarapi = XShell_Connector.get_xcalarapi()
                queries = xcalarapi.listQueries('*').queries
                # print(queries)

                name_list = []
                elapsed_list = []
                state_list = []
                for q in queries:
                    state = qstateToStr[q.state]

                    elapsed = q.milliseconds_elapsed
                    name_list.append(q.name)
                    elapsed_list.append(elapsed)
                    state_list.append(state)

                table = zip(name_list, elapsed_list, state_list)

                print(
                    tabulate(
                        table,
                        headers=['job name', 'elapsed (milliseconds)', 'state'],
                        tablefmt='psql',
                    ),
                )

        elif tokens[0] == '-cancel':
            if len(tokens) == 2 and tokens[1]:
                if confirm() is False:
                    return
                query_name = tokens[1]
                xcalarapi = XShell_Connector.get_xcalarapi()
                try:
                    xcalarapi.cancelQuery(query_name)
                except XcalarApiStatusException as e:
                    if e.status not in [
                        StatusT.StatusOperationHasFinished,
                        StatusT.StatusOperationInError,
                        StatusT.StatusOperationCancelled,
                    ]:
                        raise e
            else:
                print(help['all_usage'])
        elif tokens[0] == '-top':
            if "--interval" in tokens:
                interval_index = tokens.index("--interval")
                interval = int(tokens[interval_index + 1])
            else:
                interval = default_refresh_interval

            if "--num_procs" in tokens:
                num_procs_index = tokens.index("--num_procs")
                num_procs = int(tokens[num_procs_index + 1])
            else:
                num_procs = default_num_parallel_procs

            if "--iters" in tokens:
                iters_index = tokens.index("--iters")
                iters = int(tokens[iters_index + 1])
            else:
                iters = default_iters

            if "--verbose" in tokens:
                verbose = True
            else:
                verbose = False

            # Create a Xcalar Client object
            xcalar_top_main(
                client,
                xshell_conf.get('client').get('url'),
                xshell_conf.get('client').get('username'),
                xshell_conf.get('client').get('password'),
                interval,
                num_procs,
                verbose,
                iters
            )
        else:
            print(help['all_usage'])

    except KeyboardInterrupt:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
        return
    except Exception as err:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {err}')
        traceback.print_tb(err.__traceback__)


def load_ipython_extension(ipython, *args):
    def job_completers(self, event):
        return ['-list', '-cancel', '-top']

    ipython.register_magic_function(dispatcher, 'line', magic_name='job')
    ipython.set_hook('complete_command', job_completers, str_key='job')
    ipython.set_hook('complete_command', job_completers, str_key='%job')


def unload_ipython_extension(ipython):
    # del ipython.magics_manager.magics['table']['xyz']
    pass
