from __future__ import unicode_literals
from prompt_toolkit import shortcuts
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.styles import Style
from pygments.lexers.sql import SqlLexer

# from xcalar.solutions.singleton import Singleton_Controller
# from xcalar.solutions.tools.connector import XShell_Connector


def dispatcher(line):

    usage = """
Usage:
    leap - transfer between cluster / users/ universe.

    """

    tokens = line.split()
    # xshell_connector = XShell_Connector()
    # controller = xshell_connector.get_controller()

    try:

        # xshell_connector = XShell_Connector()
        # controller = xshell_connector.get_controller()
        # session destroy
        if len(tokens) == 0:
            cluster_style = Style.from_dict({
                'completion-menu.completion': 'bg:#008888 #ffffff',
                'completion-menu.completion.current': 'bg:#00aaaa #000000',
                'scrollbar.background': 'bg:#88aaaa',
                'scrollbar.button': 'bg:#222222',
            })
            # ===================================================================
            cluster_completer = WordCompleter([
                'xdp-mtang-132', 'xdp-sol-demo', 'xdp-edision', 'xdp-mchan-165'
            ],
                                              ignore_case=True)
            # ===================================================================
            user_completer = WordCompleter(['xdpadmin', 'admin', 'francis'],
                                           ignore_case=True)

            # ===================================================================
            universe_completer = WordCompleter(
                ['data_gen', 'francis_stock', 'stats_mart', 'var'],
                ignore_case=True)
            #
            cluster_session = PromptSession(
                lexer=PygmentsLexer(SqlLexer),
                completer=cluster_completer,
                style=cluster_style)
            user_session = PromptSession(
                lexer=PygmentsLexer(SqlLexer),
                completer=user_completer,
                style=cluster_style)
            universe_session = PromptSession(
                lexer=PygmentsLexer(SqlLexer),
                completer=universe_completer,
                style=cluster_style)

            try:
                cluser = cluster_session.prompt('   cluster > ')
                user = user_session.prompt('      user > ')
                universe = universe_session.prompt('  universe > ')
                shortcuts.clear()
                print(f'\n\n Go to: {cluser}-{user}-{universe}')
            except Exception as e:
                print(e)

            # html_completer = WordCompleter(['xdp-mtang-132', 'xdp-sol-demo', 'xdp-edison', 'xdp-mchan-165'])
            # text = prompt('Leap to cluster: ', completer=html_completer)
            # print('You said: %s' % text)

        else:
            print(usage)
    except AttributeError as ex:
        print(f'{ex}, please run "materialize" first')
    except Exception as e:
        print(e)


def load_ipython_extension(ipython, *args):
    # use ipython.register_magic_function
    #   to register "dispatcher" function
    #   make magic_name = 'http'
    ipython.register_magic_function(dispatcher, 'line', magic_name='leap')


def unload_ipython_extension(ipython):
    pass
