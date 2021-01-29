import os
from colorama import Style

from xcalar.solutions.tools.connector import XShell_Connector
from xcalar.compute.util.upload_stats_util import UploadHelper
from xcalar.compute.util.utils import tar_cz


def dispatcher(line):
    help = {
        'show_all':
            f"""
Usage:

STATS_MART
    {Style.NORMAL}stats_mart {Style.DIM}-load --date <date> --stats_root <stats_root> --num_days_prior <num_days_prior> --postfix <postfix> --upload_statsmart_project --ignore_cgroup_stats {Style.RESET_ALL} {Style.BRIGHT}\n
                                        - date - Required. Date to upload stats from. Must be of form YYYY-MM-DD. To upload all dates, set date to "all".
                                        - stats_root - Optional. Defaults to stats root of cluster you are connected to. To load from ASUP/gather_stats.py output, set stats_root to untarred location of stats dump.
                                        - num_days_prior - Optional. Default is 0.
                                        - postfix - Optional. Postfix for workbook and published tables names. This will allow for multiple StatsMart tables to reside on cluster.
                                        - upload_statsmart_project. Optional flag. If set, latest StatsMart project will be uploaded (if no StatsMart project already exists).
                                        - ignore_cgroup_stats. Optional flag.\n{Style.RESET_ALL}
"""
    }

    tokens = line.split()

    try:
        # ---------------------
        # connectivity check
        # ---------------------
        if not (XShell_Connector.self_check_client()):
            return None

        client = XShell_Connector.get_client()
        xcalarApi = XShell_Connector.get_xcalarapi()

    except AttributeError as ex:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {ex}')
    except KeyboardInterrupt:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
        return
    except Exception as e:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {e}')

    if len(tokens) == 0:
        print(help['show_all'])
        return

    try:
        question = ""

        if tokens[0] == '-load':
            if "--date" not in tokens:
                raise Exception("date is a required field.")
            date_flag_index = tokens.index("--date")
            date = tokens[date_flag_index + 1]

            question += "Uploading Stats with the following parameters:\nDate: {}\n".format(
                date)
            if "--stats_root" in tokens:
                stats_root_flag_index = tokens.index("--stats_root")
                stats_root = tokens[stats_root_flag_index + 1]
                question += "Stats Root: {}\n".format(stats_root)
            else:
                for param in client.get_config_params():
                    if param['param_name'] == 'XcalarRootCompletePath':
                        xcalar_root = param['param_value']
                stats_root = os.path.join(xcalar_root, 'DataflowStatsHistory')

            if "--num_days_prior" in tokens:
                num_days_flag_index = tokens.index("--num_days_prior")
                num_days = int(tokens[num_days_flag_index + 1])
                question += "Number of Days Prior: {}\n".format(num_days)
            else:
                num_days = 0

            if "--upload_statsmart_project" in tokens:
                upload_statsmart_workbook = True
                question += "Upload (if doesn't already exist) StatsMart Workbook: True\n"
            else:
                upload_statsmart_workbook = False

            if "--ignore_cgroup_stats" in tokens:
                load_cg_stats = False
                question += "Ignore CGroup stats: True\n"
            else:
                load_cg_stats = True

            if "--postfix" in tokens:
                postfix_flag_index = tokens.index("--postfix")
                postfix = tokens[postfix_flag_index + 1]
                question += "Postfix: {}\n".format(postfix)
            else:
                postfix = ""
            
            print(question)

            workbook_name = "StatsMart{}".format(postfix)
            try:
                workbook = client.get_workbook(workbook_name)
                session_obj = workbook.activate()
            except Exception as e:
                if "No such workbook" in str(e):
                    if upload_statsmart_workbook:
                        path = os.path.join(
                            os.getenv("XLRDIR", "/opt/xcalar/"),
                            "scripts/exampleWorkbooks/StatsMart/workbook".format(
                                workbook_name))
                        workbook = client.upload_workbook(
                            workbook_name, tar_cz(path))
                        session_obj = workbook.activate()
                        print("Successfully Uploaded Workbook with name: {}".
                                format(workbook.name))
                    else:
                        client._create_system_workbook()
                        session_obj = client._sys_session
                else:
                    raise e

            xcalarApi.setSession(session_obj)
            helper = UploadHelper(client, xcalarApi, session_obj, stats_root)
            if date == "all":
                helper.upload_all_stats(postfix=postfix, load_cg_stats=load_cg_stats, load_iostats=True)
            else:
                helper.upload_stats(
                    date, num_days, postfix=postfix, load_cg_stats=load_cg_stats, load_iostats=True)
                
        else:
            print(help['show_all'])

    except AttributeError as ex:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {ex}')


def load_ipython_extension(ipython, *args):
    def stats_mart_completers(self, event):
        return [
            '-load',
        ]

    ipython.register_magic_function(
        dispatcher, 'line', magic_name='stats_mart')
    ipython.set_hook(
        'complete_command', stats_mart_completers, str_key='stats_mart')
    ipython.set_hook(
        'complete_command', stats_mart_completers, str_key='%stats_mart')


def unload_ipython_extension(ipython):
    pass
