import pprint
from pygments.formatters import Terminal256Formatter
from pygments import highlight
from pygments.lexers import get_lexer_by_name
from colorama import Style
from xcalar.solutions.tools.connector import xshell_conf


def dispatcher(line):

    usage = f"""
Usage:
    {Style.NORMAL}property_manage {Style.DIM}-show {Style.RESET_ALL}                                                    {Style.BRIGHT}- show current propertites{Style.RESET_ALL}

    """

    tokens = line.split()

    pprint._sorted = lambda x: x
    pp = pprint.PrettyPrinter(indent=2, compact=False)

    try:

        if len(tokens) == 1 and tokens[0] == "-show":
            code = pp.pformat(xshell_conf)
            lexer = get_lexer_by_name("java", stripall=False)
            formatter = Terminal256Formatter()
            result = highlight(code, lexer, formatter)
            print(result)
        else:
            print(usage)
    except AttributeError as ex:
        print(f"{Style.DIM}Warning: {Style.RESET_ALL} {ex}")
    except Exception as e:
        print(f"{Style.DIM}Warning: {Style.RESET_ALL} {e}")


def load_ipython_extension(ipython, *args):
    ipython.register_magic_function(
        dispatcher, "line", magic_name="property_manager")


def unload_ipython_extension(ipython):
    pass
