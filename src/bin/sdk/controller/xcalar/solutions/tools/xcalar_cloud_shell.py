import IPython
import os
import sys
from IPython.terminal.prompts import Prompts, Token
from traitlets.config import Config


class XcalarPrompts(Prompts):
    def in_prompt_tokens(self, cli=None):
        return [
            (Token.Prompt, 'xcalar ['),
            (Token.PromptNum, str(self.shell.execution_count)),
            (Token.Prompt, ']> '),
        ]


if __name__ == '__main__':
    sys.path.append('/mnt/xcalar/pysite')

    pid = os.getpid()

    config = Config()
    config.InteractiveShellApp.extensions = ['autoreload']
    config.InteractiveShellApp.exec_lines = ['%autoreload 2']
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
    config.InteractiveShellApp.extensions.append(
        'xcalar.solutions.tools.extensions.francis_ext')

    config.InteractiveShellApp.exec_lines = [
    # f"""get_ipython().system = lambda *args: arg if args not in ['ssh', 'df'] else print('command not found!')  """,
        '!export PYTHONWARNINGS="ignore:OpenSSL version 1.0.1"',
        '!export XLR_PYSDK_VERIFY_SSL_CERT="false"',
        '!export PIP_REQUIRE_VIRTUALENV=true',
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
        'alias source source',
        'clear',
        'cd',
        'import warnings',
        'import os',
        'warnings.simplefilter("ignore")',
        'from colorama import Style, Fore',
        'from colorama import Style',
        'mkdir -p /var/log/xcalar/xshell',
        'logstart -t -q -o /var/log/xcalar/xshell/audit.log rotate',    # mode: append, backup, global, over, rotate
        'print(f"\\nWelcome to Xcalar Shell! Please type {Fore.YELLOW}xcalar{Style.RESET_ALL} to show menu.")',
    ]

    config.InteractiveShellApp.extensions = [
    # --------- xcalar cloud shell extensions ----------------
        'xcalar.solutions.tools.extensions.xcalar_cloud_commands_ext',
    # ---------- gs ------------------------------------------
        'xcalar.solutions.tools.extensions.connect_ext',
        'xcalar.solutions.tools.extensions.use_ext',
        'xcalar.solutions.tools.extensions.controller_app_ext',
    # ---------- marts--- ------------------------------------
        'xcalar.solutions.tools.extensions.logmart_ext',
        'xcalar.solutions.tools.extensions.stats_mart_ext',
    # ---------- utilites ------------------------------------
        'xcalar.solutions.tools.extensions.auto_confirm_ext',
        'xcalar.solutions.tools.extensions.systemstat_ext',
        'xcalar.solutions.tools.extensions.systemctl_ext',
        'xcalar.solutions.tools.extensions.config_editor_ext',
        'xcalar.solutions.tools.extensions.tmux_ext',
        'xcalar.solutions.tools.extensions.pip_ext',
        'xcalar.solutions.tools.extensions.xcalar_cluster_ext',
    # ---------- banned --------------------------------------
        'xcalar.solutions.tools.extensions.ssh_ext',
    # ---------- hide   --------------------------------------
    # ---------- test ----------------------------------------
    ]

    IPython.start_ipython(argv=[], config=config)
