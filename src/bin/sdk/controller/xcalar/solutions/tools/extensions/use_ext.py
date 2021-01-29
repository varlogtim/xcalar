# use_ext.py
import json
from colorama import Style
from xcalar.solutions.tools.connector import XShell_Connector
import os
import traceback


def dispatcher(line):
    usage = f"""
Usage:
    {Style.NORMAL}use {Style.DIM}<session_id>{Style.RESET_ALL}                                                    {Style.BRIGHT}- assigned session_id{Style.RESET_ALL}
    {Style.NORMAL}use {Style.DIM}-session_id <session_id> -properties <file_path>{Style.RESET_ALL}               {Style.BRIGHT}- assigned session_id and properties.{Style.RESET_ALL}

    """

    tokens = line.split()

    if len(tokens) == 0:
        print(usage)
        return

    try:
        # ---------------------
        # connectivity check
        # ---------------------
        if not (XShell_Connector.self_check_client()):
            return None

        xshell_connector = XShell_Connector()

        if len(tokens) == 1:
            session_id = tokens[0]

            xshell_connector.update_xshell_controller(
                universe_id=session_id,
                session_name=session_id,
                properties_file_path=None,
            )

        elif len(tokens) == 3 and tokens[1] == "-properties":
            session_id = tokens[0]
            properties_file_path = tokens[2]

            if not os.path.exists(properties_file_path):
                print(f"properties file {properties_file_path} not exists.")
                return

            XShell_Connector.initializeLogger(session_id, properties_file_path)
            xshell_connector.update_xshell_controller(
                universe_id=session_id,
                session_name=session_id,
                properties_file_path=properties_file_path,
            )

        elif len(tokens) == 4:

            if tokens[0] == "-session_id" and tokens[2] == "-properties":
                session_id = tokens[1]
                properties_file_path = tokens[3]

            if tokens[2] == "-session_id" and tokens[0] == "-properties":
                session_id = tokens[3]
                properties_file_path = tokens[1]

            if not os.path.exists(properties_file_path):
                print(f"properties file {properties_file_path} not exists.")
                return

            XShell_Connector.initializeLogger(session_id, properties_file_path)
            xshell_connector.update_xshell_controller(
                universe_id=session_id,
                session_name=session_id,
                properties_file_path=properties_file_path,
            )

        else:
            print(usage)

    except KeyboardInterrupt:
        print(f"{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.")
        return
    except Exception as err:
        print(f"{Style.DIM}Warning: {Style.RESET_ALL} {err}")
        traceback.print_tb(err.__traceback__)


def load_ipython_extension(ipython, *args):
    def auto_completers(self, event):
        return [
            "-session_id",
            "-properties",
        ]

    ipython.register_magic_function(dispatcher, "line", magic_name="use")
    ipython.set_hook("complete_command", auto_completers, str_key="use")
    ipython.set_hook("complete_command", auto_completers, str_key="%use")


def unload_ipython_extension(ipython):
    pass
