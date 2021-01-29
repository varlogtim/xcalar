from xcalar.solutions.tools.connector import XShell_Connector

# from xcalar.solutions.singleton import Singleton_Controller
from xcalar.solutions.xd_bridge import XDBridge
from IPython.core.magic import Magics, magics_class, line_magic, cell_magic
from .action_confirm import confirm
from colorama import Style
from xcalar.solutions.uber_dispatcher import UberDispatcher

# : )
help = {
    'show_all':
        f"""
Usage:
    {Style.NORMAL}xdbridge {Style.DIM}-linkins_universe{Style.RESET_ALL}                                        {Style.BRIGHT}- creates linkins pointing to all universe base/delta tables.{Style.RESET_ALL}
    {Style.NORMAL}xdbridge {Style.DIM}-linkins_session{Style.RESET_ALL}                                         {Style.BRIGHT}- creates linkins pointing to all latest session tables.{Style.RESET_ALL}
    {Style.NORMAL}xdbridge {Style.DIM}-linkins_refiner  <refiner_name>{Style.RESET_ALL}                         {Style.BRIGHT}- creates linkins pointing to all tables required by a refiner.{Style.RESET_ALL}
    {Style.NORMAL}xdbridge {Style.DIM}-params_universe{Style.RESET_ALL}                                         {Style.BRIGHT}- creates parameterized linkins pointing to all universe base/delta tables.{Style.RESET_ALL}
    {Style.NORMAL}xdbridge {Style.DIM}-params_session{Style.RESET_ALL}                                          {Style.BRIGHT}- creates parameterized linkins pointing to all latest session tables.{Style.RESET_ALL}
    {Style.NORMAL}xdbridge {Style.DIM}-params_refiner   <refiner_name>{Style.RESET_ALL}                         {Style.BRIGHT}- creates parameterized linkins pointing to all tables required by a refiner.{Style.RESET_ALL}
    """,
    'xdbridge_linkins_universe':
        f""""
Usage:
    {Style.NORMAL}xdbridge {Style.DIM}-linkins_universe{Style.RESET_ALL}                     {Style.BRIGHT}- creates linkins pointing to all universe base/delta tables.{Style.RESET_ALL}
    """,
    'xdbridge_linkins_session':
        f""""
Usage:
    {Style.NORMAL}xdbridge {Style.DIM}-linkins_session{Style.RESET_ALL}                      {Style.BRIGHT}- creates linkins pointing to all latest session tables.{Style.RESET_ALL}
    """,
    'xdbridge_linkins_refiner':
        f""""
Usage:
    {Style.NORMAL}xdbridge {Style.DIM}-linkins_refiner  <refiner_name>{Style.RESET_ALL}      {Style.BRIGHT}- creates linkins pointing to all tables required by a refiner.{Style.RESET_ALL}
    """,
    'xdbridge_params_universe':
        f""""
Usage:
    {Style.NORMAL}xdbridge {Style.DIM}-params_universe{Style.RESET_ALL}                      {Style.BRIGHT}- creates parameterized linkins pointing to all universe base/delta tables.{Style.RESET_ALL}
    """,
    'xdbridge_params_session':
        f""""
Usage:
    {Style.NORMAL}xdbridge {Style.DIM}-params_session{Style.RESET_ALL}                       {Style.BRIGHT}- creates parameterized linkins pointing to all latest session tables.{Style.RESET_ALL}
    """,
    'xdbridge_params_refiner':
        f""""
Usage:
    {Style.NORMAL}xdbridge {Style.DIM}-params_refiner   <refiner_name>{Style.RESET_ALL}      {Style.BRIGHT}- creates parameterized linkins pointing to all tables required by a refiner.{Style.RESET_ALL}
    """,
}


##
##
##
@line_magic
def power_on(str):
    xm = Xman()
    xm.do_abc(str)


@magics_class
class Xman(Magics):
    def __init__(self):
        pass

    @line_magic
    def do_abc(self, args):
        print(f'I just did abc.{args}')

    @cell_magic
    def do_xyz(self, args):
        print('I just done xyz.')

    @staticmethod
    def get_instance_for_ipython(ipython):
        try:
            magics = Xman()
            return magics
        except Exception as e:
            print(
                f'{Style.DIM}Warning: {Style.RESET_ALL} Error: Failed to load {__name__} extension\n{e}'
            )

def update_orchestrator(client, orchestrator, tableMap, shared_universe):
    for t_name in shared_universe:
        shared_universe_id = shared_universe[t_name]['from_universe_id']
        shared_orchestrator = XShell_Connector.get_orchestrator(shared_universe_id)
        shared_tableMap = shared_orchestrator.watermark.mapObjects()
        if t_name not in tableMap:
            #update flat_universe with ref tabls from the shared session
            orchestrator.flat_universe['tables'][t_name] = shared_orchestrator.flat_universe['tables'][t_name]
            tbl = shared_tableMap.get(t_name, None)
            t_info = shared_orchestrator.watermark.getInfo(t_name, shared_tableMap)
            if tbl is not None:
                tbl_name = f'/tableName/{client.get_session(shared_universe_id).session_id}/{tbl}'
                # update watermark with references to tables from the shared session
                if t_info['isDelta']:
                    orchestrator.watermark.setDelta(t_name, tbl_name)
                else:
                    orchestrator.watermark.setBase(t_name, tbl_name)
    return orchestrator

def update_tableMap(client, tableMap, table_names, shared_universe):
    rMap = {}
    for table_name in table_names:
        if table_name in tableMap:
            rMap[table_name] = tableMap[table_name]
        else:
            if table_name.endswith('delta'):
                t_name = table_name.rsplit('_', 1)[0]
            else:
                t_name = table_name
            shared_universe_id = shared_universe[t_name]['from_universe_id']
            shared_orchestrator = XShell_Connector.get_orchestrator(shared_universe_id)
            shared_tableMap = shared_orchestrator.watermark.mapObjects()
            tbl = shared_tableMap.get(table_name, None)
            if tbl is not None:
                tbl = f'/tableName/{client.get_session(shared_universe_id).session_id}/{tbl}'
            rMap[table_name] = tbl
    return rMap
# @decorate_response
def dispatcher(line):
    tokens = line.split()
    dfname = 'Input Data'

    if len(tokens) == 0:
        print(help['show_all'])
        return

    try:
        if not (XShell_Connector.self_check_client()
                and XShell_Connector.self_check_controller()):
            return None

        session = XShell_Connector.get_session_obj()
        client = XShell_Connector.get_client()

        xcalarApi = XShell_Connector.get_xcalarapi()

        # xdbridge
        if tokens[0] == '-linkins_universe':
            if confirm() is False:
                return
            orchestrator = XShell_Connector.get_orchestrator(
                XShell_Connector.get_universe_id())
            tableMap = orchestrator.watermark.mapObjects()
            shared_universe = orchestrator.flat_universe['universeRefs']
            if shared_universe:
                orchestrator = update_orchestrator(client, orchestrator,
                                                   tableMap, shared_universe)
            xdbridge = XDBridge(orchestrator=orchestrator)
            xdbridge.updateSources(mode='session')
            what = 'all universe tables'
            print(
                f'Datafow {dfname} with linkins pointing to {what} was genertated in {session.name}'
            )

        elif tokens[0] == '-linkins_session':
            if confirm() is False:
                return
            dispatcher = UberDispatcher(
                xcalarClient=client, session=session, xcalarApi=xcalarApi)
            xdbridge = XDBridge(dispatcher=dispatcher)
            xdbridge.updateSources(mode='session')
            what = 'current session tables'
            print(
                f'Datafow {dfname} with linkins pointing to {what} was genertated in {session.name}'
            )

        elif tokens[0] == '-linkins_refiner':
            if len(tokens) == 2 and tokens[1]:
                if confirm() is False:
                    return
                refiner_name = tokens[1]
                orchestrator = XShell_Connector.get_orchestrator(
                    XShell_Connector.get_universe_id())
                watermark = orchestrator.watermark
                table_names = orchestrator.flat_universe['tables'][refiner_name]['app'][
                    'meta']['inputs'].keys()
                shared_universe = orchestrator.flat_universe['universeRefs']
                tableMap = watermark.mapObjects()
                rMap = update_tableMap(client, tableMap, table_names, shared_universe)
                xdbridge = XDBridge(
                    refiner=rMap, orchestrator=orchestrator)
                xdbridge.updateSources(mode='session')
                what = f'refiner {refiner_name} input tables'
                print(
                    f'Datafow {dfname} with linkins pointing to {what} was genertated in {session.name}'
                )
            else:
                print(help['xdbridge_linkins_refiner'])
        elif tokens[0] == '-params_universe':
            if confirm() is False:
                return
            orchestrator = XShell_Connector.get_orchestrator(
                XShell_Connector.get_universe_id())
            tableMap = orchestrator.watermark.mapObjects()
            shared_universe = orchestrator.flat_universe['universeRefs']
            if shared_universe:
                orchestrator = update_orchestrator(client, orchestrator,
                                                   tableMap, shared_universe)
            xdbridge = XDBridge(orchestrator=orchestrator)
            xdbridge.updateSources(mode='params')
            what = 'all universe tables'
            print(
                f'Datafow {dfname} with linkins pointing to {what} was genertated in {session.name}'
            )

        elif tokens[0] == '-params_session':
            if confirm() is False:
                return
            dispatcher = UberDispatcher(
                xcalarClient=client, session=session, xcalarApi=xcalarApi)
            xdbridge = XDBridge(dispatcher=dispatcher)
            xdbridge.updateSources(mode='params')
            what = 'current session tables'
            print(
                f'Datafow {dfname} with linkins pointing to {what} was genertated in {session.name}'
            )
        elif tokens[0] == '-params_refiner':
            if len(tokens) == 2 and tokens[1]:
                if confirm() is False:
                    return
                refiner_name = tokens[1]
                orchestrator = XShell_Connector.get_orchestrator(
                    XShell_Connector.get_universe_id())
                watermark = orchestrator.watermark
                table_names = orchestrator.flat_universe['tables'][refiner_name]['app'][
                    'meta']['inputs'].keys()
                shared_universe = orchestrator.flat_universe['universeRefs']
                tableMap = watermark.mapObjects()
                rMap = update_tableMap(client, tableMap, table_names, shared_universe)
                xdbridge = XDBridge(
                    refiner=rMap, orchestrator=orchestrator)
                xdbridge.updateSources(mode='params')
                what = f'refiner {refiner_name} input tables'
                print(
                    f'Datafow {dfname} with linkins pointing to {what} was genertated in {session.name}'
                )
            else:
                print(help['xdbridge_params_refiner'])
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
    # init controller (keep it)
    # singleton_controller = Singleton_Controller.getInstance()

    def xdbridge_completers(self, event):
        return [
            '-linkins_universe',
            '-linkins_session',
            '-params_universe',
            '-params_session',
        ]

    ipython.register_magic_function(dispatcher, 'line', magic_name='xdbridge')
    ipython.set_hook(
        'complete_command', xdbridge_completers, str_key='xdbridge')
    ipython.set_hook(
        'complete_command', xdbridge_completers, str_key='%xdbridge')


def unload_ipython_extension(ipython):
    pass
