from xcalar.solutions.tools.connector import XShell_Connector

# from xcalar.solutions.singleton import Singleton_Controller
from xcalar.solutions.git.git_api import GitApi
from xcalar.solutions.sdlc import SdlcManager
from colorama import Style
from xcalar.solutions.properties_adapter import PropertiesAdapter


def dispatcher(line):
    usage = f"""
Usage:
SDLC
    {Style.NORMAL}sdlc {Style.DIM}-checkin  <git_branch> <git_path> <workbook> [ <dataflow> ... ]{Style.RESET_ALL}   {Style.BRIGHT}- checkin optimized dataflows with dependent interactive dataflows to git\n{Style.RESET_ALL}
    {Style.NORMAL}sdlc {Style.DIM}-checkout <git_branch> <git_path>                             {Style.RESET_ALL}   {Style.BRIGHT}- interactive dataflows in a git branch and path.{Style.RESET_ALL}
    """

    tokens = line.split()

    # singleton_controller = Singleton_Controller.getInstance()
    # controller = singleton_controller.get_controller()

    try:
        xshell_connector = XShell_Connector()
        dispatcher = xshell_connector.get_uber_dispatcher()
        git_properties = PropertiesAdapter(
            xshell_connector.get_properties()).getGitProperties()
        gitApi = GitApi(git_properties.url, git_properties.projectId,
                        git_properties.accessToken)
        sdlc = SdlcManager(dispatcher, gitApi)
        # sdlc destroy
        if len(tokens) >= 5 and tokens[0] == '-checkin':
            branch_name = tokens[1]
            path = tokens[2]
            workbook_name = tokens[3]
            dataflow_names = tokens[4:]
            for dataflow_name in dataflow_names:
                sdlc.checkin_all_dataflows([dataflow_name],
                                           branch_name,
                                           path,
                                           workbook_name=workbook_name)
        elif len(tokens) >= 2 and tokens[0] == '-checkout':
            branch_name = tokens[1]
            path = tokens[2]

            sdlc.checkout_interactive_dataflows(
                branch_name, path, None, workbook_name=None)
        else:
            print(usage)

    except AttributeError as ex:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {ex}')
    except Exception as e:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {e}')


def load_ipython_extension(ipython, *args):
    def table_completers(self, event):
        return ['-checkin', '-checkout']

    ipython.register_magic_function(dispatcher, 'line', magic_name='sdlc')
    ipython.set_hook('complete_command', table_completers, str_key='sdlc')
    ipython.set_hook('complete_command', table_completers, str_key='%sdlc')


def unload_ipython_extension(ipython):
    pass
