import atexit
import traceback
# from xcalar.solutions.singleton import Singleton_Controller
from xcalar.solutions.tools.connector import XShell_Connector
from xcalar.solutions.controller_flags import ControllerFlags
from xcalar.solutions.controller import exitHandler
from colorama import Style
from .action_confirm import confirm
import time
from structlog import get_logger
logger = get_logger(__name__)


def dispatcher(line):

    help = {
        'show_all':
            f"""
Usage:

CONTROLLER
    {Style.NORMAL}controller {Style.DIM}-materialize{Style.RESET_ALL}                                           {Style.BRIGHT}- materializes base data into session.{Style.RESET_ALL}
    {Style.NORMAL}controller {Style.DIM}-materialize_continue{Style.RESET_ALL}                                  {Style.BRIGHT}- continues materializing base data into session.{Style.RESET_ALL}
    {Style.NORMAL}controller {Style.DIM}-incremental_start <num>{Style.RESET_ALL}                               {Style.BRIGHT}- starts running incremental (num times).{Style.RESET_ALL}
    {Style.NORMAL}controller {Style.DIM}-incremental_pause{Style.RESET_ALL}                                     {Style.BRIGHT}- pauses the controller during incremental.{Style.RESET_ALL}
    {Style.NORMAL}controller {Style.DIM}-incremental_continue{Style.RESET_ALL}                                  {Style.BRIGHT}- continues the controller during incremental.{Style.RESET_ALL}
    {Style.NORMAL}controller {Style.DIM}-init{Style.RESET_ALL}                                                  {Style.BRIGHT}- starts the controller session, reads the universe.{Style.RESET_ALL}
    {Style.NORMAL}controller {Style.DIM}-snapshot_take{Style.RESET_ALL}                                         {Style.BRIGHT}- manually snapshots the universe.{Style.RESET_ALL}
    {Style.NORMAL}controller {Style.DIM}-snapshot_list{Style.RESET_ALL}                                         {Style.BRIGHT}- lists all the snapshots.{Style.RESET_ALL}
    {Style.NORMAL}controller {Style.DIM}-snapshot_recover <timestamp>{Style.RESET_ALL}                          {Style.BRIGHT}- recovers specific snapshot{Style.RESET_ALL}
    {Style.NORMAL}controller {Style.DIM}-snapshot_recover{Style.RESET_ALL}                                      {Style.BRIGHT}- recovers latest snapshot{Style.RESET_ALL}
    {Style.NORMAL}controller {Style.DIM}-snapshot_metadata_override <metadata>{Style.RESET_ALL}                 {Style.BRIGHT}- overrides the metadata about snapshots stored in the cluster {Style.RESET_ALL}
    {Style.NORMAL}controller {Style.DIM}-snapshot_metadata_clear{Style.RESET_ALL}                               {Style.BRIGHT}- clears the stored metadata about snapshots in the cluter{Style.RESET_ALL}
    """,
    # {Style.NORMAL}controller {Style.DIM}-stop{Style.RESET_ALL}                                                  {Style.BRIGHT}- unregister the controller.{Style.RESET_ALL}
        'controller_materialize':
            f"""
Usage:
    CONTROLLER
    {Style.NORMAL}controller {Style.DIM}-materialize{Style.RESET_ALL}                    {Style.BRIGHT}- materializes base data into session.{Style.RESET_ALL}
    """,
        'controller_materialize_continue':
            f"""
Usage:
    CONTROLLER
    {Style.NORMAL}controller {Style.DIM}-materialize_continue{Style.RESET_ALL}           {Style.BRIGHT}- continues materializing base data into session.{Style.RESET_ALL}
    """,
        'controller_incremental_start':
            f"""
Usage:
    CONTROLLER
    {Style.NORMAL}controller {Style.DIM}-incremental_start <num>{Style.RESET_ALL}        {Style.BRIGHT}- starts running incremental (num times).{Style.RESET_ALL}
    """,
        'controller_incremental_pause':
            f"""
Usage:
    CONTROLLER
    {Style.NORMAL}controller {Style.DIM}-incremental_pause{Style.RESET_ALL}              {Style.BRIGHT}- pauses the controller during incremental.{Style.RESET_ALL}
    """,
        'controller_incremental_continue':
            f"""
Usage:
    CONTROLLER
    {Style.NORMAL}controller {Style.DIM}-incremental_pause{Style.RESET_ALL}              {Style.BRIGHT}- pauses the controller during incremental.{Style.RESET_ALL}
    """,
        'controller_init':
            f"""
Usage:
    CONTROLLER
    {Style.NORMAL}controller {Style.DIM}-inin{Style.RESET_ALL}                           {Style.BRIGHT}- starts the controller session, reads the universe.{Style.RESET_ALL}
    """,
        'controller_stop':
            f"""
Usage:
    CONTROLLER
    {Style.NORMAL}controller {Style.DIM}-stop{Style.RESET_ALL}                           {Style.BRIGHT}- unregister the controller.{Style.RESET_ALL}
    """,
        'controller_snapshot_take':
            f"""
Usage:
    CONTROLLER
    {Style.NORMAL}controller {Style.DIM}-snapshot_take{Style.RESET_ALL}                  {Style.BRIGHT}- manually snapshots the universe.{Style.RESET_ALL}
    """,
        'controller_snapshot_list':
            f"""
Usage:
    CONTROLLER
    {Style.NORMAL}controller {Style.DIM}-snapshot_list{Style.RESET_ALL}                  {Style.BRIGHT}- lists all the snapshots.{Style.RESET_ALL}
    """,
        'controller_snapshot_recover':
            f"""
Usage:
    CONTROLLER
    {Style.NORMAL}controller {Style.DIM}-snapshot_recover{Style.RESET_ALL}               {Style.BRIGHT}- Recover a specific snapshot (by timestamp).{Style.RESET_ALL}
    """,
        'controller_snapshot_metadata_override':
            f"""
Usage:
    CONTROLLER
    {Style.NORMAL}controller {Style.DIM}-snapshot_metadata_override <metadata>{Style.RESET_ALL}               {Style.BRIGHT}- overrides the metadata about snapshots stored in the cluster.{Style.RESET_ALL}
    """,
        'controller_snapshot_metadata_clear':
            f"""
Usage:
    CONTROLLER
    {Style.NORMAL}controller {Style.DIM}-snapshot_metadata_clear{Style.RESET_ALL}               {Style.BRIGHT}- clears the stored metadata about snapshots in the cluter{Style.RESET_ALL}
    """,
    }

    tokens = line.split()
    if len(tokens) == 0:
        print(help['show_all'])
        return
    # ---------------------
    # connectivity check
    # ---------------------
    if not (XShell_Connector.self_check_client()
            and XShell_Connector.self_check_controller()):
        return None

    XShell_Connector.self_check_client()
    xshell_connector = XShell_Connector()
    controller = xshell_connector.get_controller()
    atexit.register(exitHandler, controller=controller)

    try:

        if tokens[0] == '-materialize':
            if confirm() is False:
                return
            controller.to_materializing(event_data={'dirty': False})
        elif tokens[0] == '-materialize_continue':
            if confirm() is False:
                return
            controller.to_materializing(event_data={'dirty': True})

        elif tokens[0] == '-incremental_start':
            if len(tokens) == 2 and tokens[1]:
                if confirm() is False:
                    return
                num = int(tokens[1])
                controller.to_streaming(event_data={'num_runs': int(num)})

        elif tokens[0] == '-incremental_pause':
            controller.to_initialized()
            controllerflags = ControllerFlags(
                controller.universeId,
                controller.dispatcher.xcalarClient,
                controller.dispatcher.session,
            )
            controllerflags.pause()
        elif tokens[0] == '-incremental_continue':
            controllerflags = ControllerFlags(
                controller.universeId,
                controller.dispatcher.xcalarClient,
                controller.dispatcher.session,
            )
            controllerflags.unpause()
            controller.to_streaming(event_data={'num_runs': 0})
            print(
                'Incremental has been unpaused. \nPlease run `controller -incremental_start <num>` again.\n'
            )
        elif tokens[0] == '-init':
            controller = xshell_connector.get_controller()
            print('Controller is reinitialized.')
        elif tokens[0] == '-stop':
            if confirm() is False:
                return
            print('not yet implemented')
        elif tokens[0] == '-snapshot_take':
            if confirm() is False:
                return
            controller.snapshot()
        elif tokens[0] == '-snapshot_list':
            mgmt = controller.orchestrator.get_snapshot_management(
                controller.universeId)
            snapshots = mgmt.get_snapshots()
            for snapshot in snapshots:
                print(
                    f"Date Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(snapshot.snapshot_timestamp))}, timestamp: {Style.DIM}{snapshot.snapshot_timestamp}{Style.RESET_ALL}"
                )
        elif tokens[0] == '-snapshot_recover' and len(tokens) > 1:
            if confirm() is False:
                return
            controller.recoverFromSpecificSnapshot(tokens[1])
        elif tokens[0] == '-snapshot_recover':
            if confirm() is False:
                return
            controller.recoverFromLatestSnapshot()
        elif tokens[0] == '-snapshot_metadata_override' and len(tokens) > 1:
            if confirm() is False:
                return
            mgmt = controller.orchestrator.get_snapshot_management(
                controller.universeId)
            mgmt.override_snapshot_metadata(tokens[1])
        elif tokens[0] == '-snapshot_metadata_clear':
            if confirm() is False:
                return
            mgmt = controller.orchestrator.get_snapshot_management(
                controller.universeId)
            mgmt.clear_snapshot_metadata()
        else:
            print(help['show_all'])
    except ValueError as err:
        logger.info(f'{traceback.format_exc(err)}')
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {err}')
    except AttributeError as err:
        logger.info(f'{traceback.format_exc(err)}')
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {err}')
    except KeyboardInterrupt:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
        return
    except Exception as e:
        logger.info(f'{traceback.format_exc(e)}')
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {e}')


def load_ipython_extension(ipython, *args):
    def controller_completers(self, event):
        return [
            '-materialize', '-materialize_continue', '-incremental_start',
            '-incremental_pause', '-incremental_continue', '-init',
            '-snapshot_take', '-snapshot_list', '-snapshot_recover',
            '-snapshot_metadata_override', '-snapshot_metadata_clear'
        ]

    ipython.register_magic_function(
        dispatcher, 'line', magic_name='controller')
    ipython.set_hook(
        'complete_command', controller_completers, str_key='controller')
    ipython.set_hook(
        'complete_command', controller_completers, str_key='%controller')


def unload_ipython_extension(ipython):
    pass
