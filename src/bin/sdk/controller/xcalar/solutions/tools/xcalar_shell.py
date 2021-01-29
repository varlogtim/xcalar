import IPython
import os
import sys
import subprocess
import pkg_resources
from IPython.terminal.prompts import Prompts, Token
from traitlets.config import Config
from xcalar.solutions.tools.utilities.xcalar_config import get_xcalar_properties


class XcalarPrompts(Prompts):
    def in_prompt_tokens(self, cli=None):
        try:
            session_name = os.environ['XCALAR_SHELL_SESSION_NAME']
        except Exception:
            session_name = ''

        if session_name == '':
            colored_session_name = ''
        else:
            colored_session_name = f'({session_name})'

        return [
            (Token.Prompt, 'xcalar '),
            (Token.Token, f'{colored_session_name}'),
            (Token.Prompt, '['),
            (Token.PromptNum, str(self.shell.execution_count)),
            (Token.Prompt, ']> '),
        ]


if __name__ == '__main__':
    properties = get_xcalar_properties()
    PYSITE = properties['PYSITE']
    XCE_CONFIG = properties['XCE_CONFIG']
    XCE_LOGDIR = properties['XCE_LOGDIR']
    pid = os.getpid()
    uid = os.geteuid()
    sys.path.append(f'{PYSITE}')
    os.environ['PYTHONWARNINGS'] = 'ignore:OpenSSL version 1.0.1'
    os.environ['XLR_PYSDK_VERIFY_SSL_CERT'] = 'false'
    os.environ['IPYTHONDIR'] = f'/tmp/xcalar_shell-{uid}/.ipython'
    os.environ['ANSIBLE_LOCAL_TEMP'] = f'/tmp/xcalar_shell-{uid}/.ansible/tmp'
    # os.environ['PIP_REQUIRE_VIRTUALENV'] = 'true'
    config = Config()
    config.Application.log_format = '%(pid)[%(name)s]%(highlevel)s %(message)s'
    config.TerminalInteractiveShell.term_title_format = f'xcalar shell: {pid}'
    config.TerminalIPythonApp.display_banner = False
    config.InteractiveShell.colors = 'neutral'
    config.InteractiveShell.confirm_exit = False
    config.exit_msg = 'bye bye!'
    config.PrefilterManager.multi_line_specials = True
    config.IPCompleter.use_jedi = False
    config.IPCompleter.greedy = False
    config.TerminalInteractiveShell.prompts_class = XcalarPrompts

    config.InteractiveShellApp.exec_lines = [
        # f"""get_ipython().system = lambda *args: arg if args not in ['ssh', 'df'] else print('command not found!')  """,
        'cd',
        'alias source source',
        'alias xc2 xc2',
        'alias vi vim',
        'alias ps ps',
        'alias find find',
        'alias awk awk',
        'alias sed sed',
        'alias cut cut',
        'alias gawk gawk',
        'alias grep grep --color ',
        'alias echo echo',
        'clear',
        'import warnings',
        'import os',
        'warnings.simplefilter("ignore")',
        'from colorama import Style, Fore',
        'from colorama import Style',
        'from xcalar.solutions.singleton import Singleton_Controller',
        'from xcalar.solutions.tools.connector import XShell_Connector, xshell_conf',
        'oz = XShell_Connector()',
        """
        def Controller():
            if xshell_conf.get('client').get('client') is None:
                print(f'Please run "{Style.DIM}connect {Style.RESET_ALL}" command first. ')
                return
            if xshell_conf.get('controller').get('analyzer') is None:
                print(f'Please run "{Style.DIM}connect {Style.RESET_ALL}" command first. ')
                return

            try:
                #singleton = Singleton_Controller.getInstance()
                #return singleton.get_controller(XShell_Connector.get_universe_id(), XShell_Connector.get_properties())
                return XShell_Connector.get_controller()
            except Exception as e:
                    print(f'{e} Please run "{Style.DIM}connect {Style.RESET_ALL}" command first. ')
        """,
        f'mkdir -p /tmp/xcalar_shell-{uid}/.ipython',
        f'mkdir -p /tmp/xcalar_shell-{uid}/.ansible/tmp',
        f'mkdir -p {XCE_LOGDIR}/xshell',
        f'mkdir -p {PYSITE}',
        f'logstart -t -q -o {XCE_LOGDIR}/xshell/audit.log append',  # mode: append, backup, global, over, rotate
        'print(f"\\nWelcome to Xcalar Shell! Please type {Fore.YELLOW}xcalar{Style.RESET_ALL} to show menu.")',
    ]


    config.InteractiveShellApp.extensions = [
        'xcalar.solutions.tools.extensions.xcalar_cloud_commands_ext',
        # --------- extensions ----------------
        'xcalar.solutions.tools.extensions.analyze_ext',
        'xcalar.solutions.tools.extensions.auto_confirm_ext',
        'xcalar.solutions.tools.extensions.clr_mart_ext',
        'xcalar.solutions.tools.extensions.config_editor_ext',
        'xcalar.solutions.tools.extensions.connect_ext',
        'xcalar.solutions.tools.extensions.controller_app_ext',
        'xcalar.solutions.tools.extensions.job_ext',
        'xcalar.solutions.tools.extensions.logmart_ext',
        'xcalar.solutions.tools.extensions.monitor_ext',
        'xcalar.solutions.tools.extensions.property_manager_ext',
        'xcalar.solutions.tools.extensions.sdlc_ext',
        'xcalar.solutions.tools.extensions.session_ext',
        'xcalar.solutions.tools.extensions.sql_ext',
        'xcalar.solutions.tools.extensions.stats_mart_ext',
        'xcalar.solutions.tools.extensions.systemctl_ext',
        'xcalar.solutions.tools.extensions.systemstat_ext',
        'xcalar.solutions.tools.extensions.table_ext',
        'xcalar.solutions.tools.extensions.tmux_ext',
        'xcalar.solutions.tools.extensions.use_ext',
        'xcalar.solutions.tools.extensions.xdbridge_ext',
        'xcalar.solutions.tools.extensions.xpip_ext',
        'xcalar.solutions.tools.extensions.xcalar_cluster_ext',
        # ---------- container --------------
        'xcalar.container.kafka.kafka_app_ext',
        # ---------- banned --------------
        'xcalar.solutions.tools.extensions.ssh_ext',
    ]

    IPython.start_ipython(argv=[], config=config)
