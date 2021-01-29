from colorama import Style
from .action_confirm import confirm
import time
from xcalar.solutions.tools.connector import XShell_Connector, xshell_conf

#


def dispatcher(line):

    help = {
        'show_all':
            f"""
Usage:

CONTROLLER_APP
    {Style.NORMAL}controller_app {Style.DIM}-init{Style.RESET_ALL}                                                                                                   {Style.BRIGHT}- starts the controller app session, reads the universe in the controller app.{Style.RESET_ALL}
    {Style.NORMAL}controller_app {Style.DIM}-state{Style.RESET_ALL}                                                                                                  {Style.BRIGHT}- gets the current state of the controller app{Style.RESET_ALL}
    {Style.NORMAL}controller_app {Style.DIM}-stop{Style.RESET_ALL}                                                                                                   {Style.BRIGHT}- cancels the controller app{Style.RESET_ALL}
    {Style.NORMAL}controller_app {Style.DIM}-materialize{Style.RESET_ALL}                                                                                            {Style.BRIGHT}- materializes base data into session in the controller app{Style.RESET_ALL}
    {Style.NORMAL}controller_app {Style.DIM}-materialize --using_latest_snapshot_from_universe_id <universe_id>{Style.RESET_ALL}                                     {Style.BRIGHT}- materializes starting from another univere's snapshot.{Style.RESET_ALL}
    {Style.NORMAL}controller_app {Style.DIM}-materialize_continue{Style.RESET_ALL}                                                                                   {Style.BRIGHT}- continues materializing the base data into a dirty session in the controller app{Style.RESET_ALL}
    {Style.NORMAL}controller_app {Style.DIM}-incremental{Style.RESET_ALL}                                                                                            {Style.BRIGHT}- starts running incremental in the controller app{Style.RESET_ALL}
    {Style.NORMAL}controller_app {Style.DIM}-incremental <num>{Style.RESET_ALL}                                                                                      {Style.BRIGHT}- starts running incremental for a set number of times in the controller app{Style.RESET_ALL}
    {Style.NORMAL}controller_app {Style.DIM}-adjustment --tables <table1_name,table2_name,table3_name> --action <[add, drop, replace]>{Style.RESET_ALL}              {Style.BRIGHT}- adjusts the multiple universes(split by comma) with the criteria{Style.RESET_ALL}
    {Style.NORMAL}controller_app {Style.DIM}-adjustment --offset_table --topic <topic_name> --partitions <partition_list> --new_offset <new_offset>{Style.RESET_ALL} {Style.BRIGHT}- adjusts the offset table with the criteria{Style.RESET_ALL}
    {Style.NORMAL}controller_app {Style.DIM}-incremental_pause{Style.RESET_ALL}                                                                                      {Style.BRIGHT}- pauses the incremental in the controller app{Style.RESET_ALL}
    {Style.NORMAL}controller_app {Style.DIM}-snapshot_take{Style.RESET_ALL}                                                                                          {Style.BRIGHT}- manually snapshots the universe{Style.RESET_ALL}
    {Style.NORMAL}controller_app {Style.DIM}-snapshot_list{Style.RESET_ALL}                                                                                          {Style.BRIGHT}- lists all the snapshots.{Style.RESET_ALL}
    {Style.NORMAL}controller_app {Style.DIM}-snapshot_recover <timestamp>{Style.RESET_ALL}                                                                           {Style.BRIGHT}- recovers specific snapshot{Style.RESET_ALL}
    {Style.NORMAL}controller_app {Style.DIM}-snapshot_recover{Style.RESET_ALL}                                                                                       {Style.BRIGHT}- recovers latest snapshot{Style.RESET_ALL}
    {Style.NORMAL}controller_app {Style.DIM}-destroy{Style.RESET_ALL}                                                                                                {Style.BRIGHT}- destroy the universe{Style.RESET_ALL}
    """,
    # {Style.NORMAL}controller {Style.DIM}-stop{Style.RESET_ALL}                                                  {Style.BRIGHT}- unregister the controller.{Style.RESET_ALL}
        'controller_app_materialize':
            f"""
Usage:
    CONTROLLER_APP
    {Style.NORMAL}controller_app {Style.DIM}-materialize{Style.RESET_ALL}                    {Style.BRIGHT}- materializes base data into session in the controller app.{Style.RESET_ALL}
    """,
        'controller_app_init':
            f"""
Usage:
    CONTROLLER_APP
    {Style.NORMAL}controller_app {Style.DIM}-init{Style.RESET_ALL}                           {Style.BRIGHT}- starts the controller session, reads the universe in the controller app.{Style.RESET_ALL}
    """,
        'controller_app_stop':
            f"""
Usage:
    CONTROLLER_APP
    {Style.NORMAL}controller_app {Style.DIM}-stop{Style.RESET_ALL}                           {Style.BRIGHT}- unregisters the controller app.{Style.RESET_ALL}
    """,
        'controller_app_pause':
            f"""
Usage:
    CONTROLLER_APP
    {Style.NORMAL}controller_app {Style.DIM}-incremental_pause{Style.RESET_ALL}                          {Style.BRIGHT}- pauses the incremental in the controller app.{Style.RESET_ALL}
    """,
        'controller_app_incremental':
            f"""
Usage:
    CONTROLLER
    {Style.NORMAL}controller_app {Style.DIM}-incremental{Style.RESET_ALL}                    {Style.BRIGHT}- starts running incremental in the controller app.{Style.RESET_ALL}
    """,
        'controller_app_adjustment':
            f"""
Usage:
    CONTROLLER
    {Style.NORMAL}controller_app {Style.DIM}-adjustment{Style.RESET_ALL}                    {Style.BRIGHT}- adjusts universe or offset tables.{Style.RESET_ALL}
    """,
        'controller_app_snapshot':
            f"""
Usage:
    CONTROLLER
    {Style.NORMAL}controller_app {Style.DIM}-snapshot{Style.RESET_ALL}                    {Style.BRIGHT}- recovers the snapshot{Style.RESET_ALL}
    """,
    }

    tokens = line.split()

    if len(tokens) == 0:
        print(help['show_all'])
        return

    try:
        # ---------------------
        # connectivity check
        # ---------------------
        if not (XShell_Connector.self_check_client()
                and XShell_Connector.self_check_controller()):
            return None

        xshell_connector = XShell_Connector()
        xshell_connector.setup()
        client = xshell_connector.get_client()
        xcalarApi = xshell_connector.get_xcalarapi()
        universeId = xshell_conf.get('controller').get('universe_id')

        if universeId is None:
            raise ValueError(
                f' Please run "{Style.DIM}use {Style.RESET_ALL}" command to get xcalarapi instance. '
            )

        if (tokens[0] == '-materialize' and len(tokens) == 3
                and tokens[1] == '--using_latest_snapshot_from_universe_id'):
            if confirm() is False:
                return

            appClient = None
            appClient = xshell_connector.get_appClient()
            if appClient is not None:
                from_universe_id = tokens[2]
                appClient.materialize_from_universe_id(from_universe_id)
        elif tokens[0] == '-materialize' and len(tokens) == 1:
            if confirm() is False:
                return

            appClient = None
            appClient = xshell_connector.get_appClient()
            if appClient is not None:
                appClient.materialize()
        elif tokens[0] == '-materialize_continue' and len(tokens) == 1:
            if confirm() is False:
                return

            appClient = None
            appClient = xshell_connector.get_appClient()
            if appClient is not None:
                appClient.materialize_continue()
        elif tokens[0] == '-init' and len(tokens) == 1:

            # propertiesAdapter = PropertiesAdapter(
            #     os.environ["XCALAR_SHELL_PROPERTIES_FILE"]
            # )
            # xcalarProperties = propertiesAdapter.getXcalarProperties()
            # controllerAppProps = propertiesAdapter.getControllerAppProps()
            # client = get_client(
            #     xcalarProperties.url,
            #     xcalarProperties.username,
            #     xcalarProperties.password,
            # )

            # xcalarApi = get_xcalarapi(
            #     xcalarProperties.url,
            #     xcalarProperties.username,
            #     xcalarProperties.password,
            # )
            # sessionName = xshell_conf.get("controller").get("universe_id")
            # session = get_or_create_session(client, sessionName)
            # xcalarApi.setSession(session)
            try:
                appClient = xshell_connector.get_appClient()
            except Exception:
                xshell_connector.update_xshell_appClient(
                    client, universeId, xcalarApi)
                appClient = xshell_connector.get_appClient()
            if appClient is not None:
                appClient.runControllerApp()
                num_try = (xshell_conf.get('controllerApp').get('stateChange').
                           get('numTries'))
                num = 0
                state = None
                while num < num_try:
                    state = appClient.get_state()
                    if state is not None:
                        break
                    num += 1
                    print(f'Controller is initializing...')
                    time.sleep(
                        xshell_conf.get('controllerApp').get('stateChange').
                        get('sleepIntervalInSec'))
                print(f'controller state is {state}')
        elif tokens[0] == '-stop' and len(tokens) == 1:
            if confirm() is False:
                return

            appClient = None
            appClient = xshell_connector.get_appClient()
            if appClient is not None:
                appClient.stop()
                xshell_connector.release_appClient()
        elif tokens[0] == '-incremental' and len(tokens) == 1:

            if confirm() is False:
                return

            appClient = None
            appClient = xshell_connector.get_appClient()
            if appClient is not None:
                state = appClient.get_state()
                if not state:
                    print('State transition is in progress, please check controller -state')
                    return
                elif state == 'streaming':
                    print('Current controller is already in streaming state')
                    return
                appClient.incremental()
        elif tokens[0] == '-incremental' and len(tokens) == 2:
            if confirm() is False:
                return
            appClient = None

            # num_runs = int(tokens[1])
            appClient = None
            appClient = xshell_connector.get_appClient()
            if appClient is not None:
                state = appClient.get_state()
                if not state:
                    print('State transition is in progress, please check controller -state')
                    return
                elif state == 'streaming':
                    print('Current controller is already in streaming state')
                    return
                num_runs = int(tokens[1])
                appClient.incremental_num_runs(num_runs)
        elif (tokens[0] == '-adjustment' and len(tokens) == 5
              and tokens[1] == '--tables' and tokens[3] == '--action'):
            if confirm() is False:
                return

            appClient = None
            appClient = xshell_connector.get_appClient()
            if appClient is not None:
                state = appClient.get_state()
                if (state == 'initialized' or state == 'materialized'):
                    table_names = tokens[2].split(',')
                    action = tokens[4]
                    appClient.adjust_table(table_names, action)
                elif state == 'failed':
                    print('Adjustment fails, need to run controller_app -stop to reset and restart a new app')
                    print('Or run controller_app -destroy to destroy the universe')
                else:
                    print(
                        'need to run controller_app -incremental_pause first')
        elif (tokens[0] == '-adjustment' and len(tokens) == 8
              and tokens[1] == '--offset_table' and tokens[2] == '--topic'
              and tokens[4] == '--partitions' and tokens[6] == '--new_offset'):
            if confirm() is False:
                return

            appClient = None
            appClient = xshell_connector.get_appClient()
            if appClient is not None:
                state = appClient.get_state()
                if (state == 'initialized' or state == 'materialized'):
                    topic_name = tokens[3]
                    partition_list = tokens[5]
                    new_offset = tokens[7]
                    appClient.adjust_offset_table(topic_name, partition_list,
                                                  new_offset)
                elif state == 'failed':
                    print('Adjustment fails, need to run controller_app -stop to reset and restart a new app')
                    print('Or run controller_app -destroy to destroy the universe')
                else:
                    print(
                        'need to run controller_app -incremental_pause first')
        elif tokens[0] == '-incremental_pause' and len(tokens) == 1:
            if confirm() is False:
                return

            appClient = None
            appClient = xshell_connector.get_appClient()
            if appClient is not None:
                appClient.pause()
        elif tokens[0] == '-snapshot_take' and len(tokens) == 1:
            if confirm() is False:
                return

            appClient = None
            appClient = xshell_connector.get_appClient()
            if appClient is not None:
                appClient.snapshot()
        elif tokens[0] == '-destroy' and len(tokens) == 1:
            if confirm() is False:
                return
            appClient = None

            appClient = None
            appClient = xshell_connector.get_appClient()
            if appClient is not None:
                appClient.reset()
        elif tokens[0] == '-state' and len(tokens) == 1:
            appClient = None
            appClient = xshell_connector.get_appClient()
            if appClient is not None:
                if appClient.check_alive():
                    state = appClient.get_state()
                    if state:
                        print(
                            f'Current controller state is {appClient.get_state()}'
                        )
                    else:
                        transitStates = appClient.get_transit_state()
                        if transitStates:
                            print('State transition is in progress...')
                            print(
                                f'Previous controller state is {transitStates["previousState"]}'
                            )
                            print(
                                f'Next controller state is {transitStates["nextState"]}'
                            )
                        else:
                            raise Exception('Failed to find state')
                else:
                    print(
                        'Current controller died, run controller_app -stop to clean up'
                    )
        elif tokens[0] == '-snapshot_list' and len(tokens) == 1:
            appClient = None
            appClient = xshell_connector.get_appClient()
            if appClient is not None:
                snapshots = appClient.listSnapshots()
                for snapshot in snapshots:
                    print(
                        f"Date Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(snapshot.snapshot_timestamp))}, timestamp: {Style.DIM}{snapshot.snapshot_timestamp}{Style.RESET_ALL}"
                    )
        elif tokens[0] == '-snapshot_recover':
            timestamp = None
            if len(tokens) == 1:
                timestamp = ''
            elif len(tokens) == 2:
                timestamp = tokens[1]

            else:
                print(help['show_all'])
                return
            if confirm() is False:
                return
            appClient = None
            appClient = xshell_connector.get_appClient()
            if appClient is not None:
                if timestamp != '':
                    appClient.recoverFromSpecificSnapshot(timestamp)
                else:
                    appClient.recoverFromLatestSnapshot()
        else:
            print(help['show_all'])
    except AttributeError as err:
        print(err)
    except KeyboardInterrupt:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
        return
    except Exception as e:
        print(e)


def load_ipython_extension(ipython, *args):
    def controller_app_completers(self, event):
        return [
            '-materialize',
            '-materialize_continue',
            '-init',
            '-state',
            '-stop',
            '-incremental',
            '-adjustment',
            '-incremental_pause',
            '-snapshot_take',
            '-snapshot_list',
            '-snapshot_recover',
            '-destroy',
        ]

    ipython.register_magic_function(
        dispatcher, 'line', magic_name='controller_app')
    ipython.set_hook(
        'complete_command',
        controller_app_completers,
        str_key='controller_app')
    ipython.set_hook(
        'complete_command',
        controller_app_completers,
        str_key='%controller_app')


def unload_ipython_extension(ipython):
    pass
