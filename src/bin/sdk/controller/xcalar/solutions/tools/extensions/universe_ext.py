# SF-482

# from xcalar.solutions.singleton import Singleton_Controller
from xcalar.solutions.tools.connector import XShell_Connector
from colorama import Style
from tabulate import tabulate
from .action_confirm import confirm


def dispatcher(line):
    # {Style.DIM}-describe                 <table_alias>   <refiner>{Style.RESET_ALL}
    # {Style.NORMAL}universe {Style.DIM}-table_list{Style.RESET_ALL}                                              {Style.BRIGHT}- shows universe table list.{Style.RESET_ALL}
    # {Style.NORMAL}universe {Style.DIM}-recover                      <timestamp>{Style.RESET_ALL}                {Style.BRIGHT}- recovers the universe from the latest/old snapshot.{Style.RESET_ALL}
    help = {
        "show_all":
            f"""
Usage:

UNIVERSE
    {Style.NORMAL}universe {Style.DIM}-describe                 <table_alias>{Style.RESET_ALL}                  {Style.BRIGHT}- shows current universe info like name, etc.{Style.RESET_ALL}
    {Style.NORMAL}universe {Style.DIM}-adjust_drop              <table_alias>{Style.RESET_ALL}                  {Style.BRIGHT}- drops a table along with metadata.{Style.RESET_ALL}
    {Style.NORMAL}universe {Style.DIM}-adjust_add               <table_alias>{Style.RESET_ALL}                  {Style.BRIGHT}- adds a table.{Style.RESET_ALL}
    {Style.NORMAL}universe {Style.DIM}-adjust_replace           <table_alias>{Style.RESET_ALL}                  {Style.BRIGHT}- replaces a table.{Style.RESET_ALL}
    {Style.NORMAL}universe {Style.DIM}-adjust_offsets    <topic> <partitions> <new_offset>{Style.RESET_ALL}  {Style.BRIGHT}- adjusts offset for a topic on one or several partitions.{Style.RESET_ALL}
""",
        "universe_table_list":
            f"""
Usage:
    UNIVERSE
    {Style.NORMAL}universe {Style.DIM}-table_list{Style.RESET_ALL}                       {Style.BRIGHT}- shows universe table list.{Style.RESET_ALL}
        """,
        "universe_describe":
            f"""
Usage:
    UNIVERSE
    {Style.NORMAL}universe {Style.DIM}-describe{Style.RESET_ALL} <table>                 {Style.BRIGHT}- shows current universe info like name, etc.{Style.RESET_ALL}
             {Style.DIM}-describe{Style.RESET_ALL} <refiner> <table>
        """,
        "universe_recover":
            f"""
Usage:
    UNIVERSE
    {Style.NORMAL}universe {Style.DIM}-recover <timestamp>{Style.RESET_ALL}              {Style.BRIGHT}- recovers the universe from the latest/old snapshot.{Style.RESET_ALL}
        """,
        "universe_adjust_drop":
            f"""
Usage:
    UNIVERSE
    {Style.NORMAL}universe {Style.DIM}-adjust_drop <alias name/>{Style.RESET_ALL}    {Style.BRIGHT}- drops a table along with metadata.{Style.RESET_ALL}
        """,
        "universe_adjust_add":
            f"""
Usage:
    UNIVERSE
    {Style.NORMAL}universe {Style.DIM}-adjust_add  <alias name/>{Style.RESET_ALL}    {Style.BRIGHT}- adds a table.{Style.RESET_ALL}
        """,
        "universe_adjust_replace":
            f"""
Usage:
    UNIVERSE
    {Style.NORMAL}universe {Style.DIM}-adjust_replace <alias name/>{Style.RESET_ALL} {Style.BRIGHT}- replaces a table.{Style.RESET_ALL}
        """,
        "universe_adjust_offsets":
            f"""
Usage:
    UNIVERSE
        {Style.NORMAL}universe {Style.DIM}-adjust_offsets <topic> <partitions> <new_offset>{Style.RESET_ALL}  {Style.BRIGHT}- adjusts offset for a topic on one or several partitions.{Style.RESET_ALL}
        """,
    }

    tokens = line.split()
    # singleton_controller = Singleton_Controller.getInstance()
    # controller = singleton_controller.get_controller()

    if len(tokens) == 0:
        print(help["show_all"])
        return

    try:
        xshell_connector = XShell_Connector()
        controller = xshell_connector.get_controller()
        # universe
        if tokens[0] == "-table_list":
            tables = controller.universe.universe.keys()

            t = zip(tables)
            print(tabulate(
                t,
                headers=["name"],
                tablefmt="psql",
            ))

        elif tokens[0] == "-describe":
            if len(tokens) == 2 and tokens[1]:
                table_name = tokens[1]

                # json_object = json.loads(json.dumps(controller.universe.sourceDefs))
                # json_str = json.dumps(json_object, indent=4, sort_keys=True)
                # print(highlight(json_str, JsonLexer(), TerminalFormatter()))

                data = controller.universe.getTable(table_name)

                for mykey in data.keys():
                    dict_keys = data[mykey].keys()
                    dict_values = data[mykey].values()
                    t = zip(dict_keys, dict_values)
                    print(
                        f"\n{mykey}: \n",
                        tabulate(
                            t,
                            headers=["name", "type"],
                            tablefmt="psql",
                        ),
                    )
            elif len(tokens) == 3 and tokens[1] and tokens[2]:
                data = controller.universe.getTable(tokens[1], tokens[2])
                for mykey in data.keys():
                    dict_keys = data[mykey].keys()
                    dict_values = data[mykey].values()
                    t = zip(dict_keys, dict_values)
                    print(
                        f"\n{mykey}: \n",
                        tabulate(
                            t,
                            headers=["name", "type"],
                            tablefmt="psql",
                        ),
                    )
            else:
                print(help["universe_describe"])

        elif tokens[0] == "-recover":
            if confirm() is None:
                return
            controller.recoverFromLatestSnapshot()
            print("universe recover from latest snapshot.")

        elif tokens[0] == "-adjust_drop":
            if len(tokens) == 2 and tokens[1]:
                if confirm() is None:
                    return
                controller.adjustTable([tokens[1]], "drop")
                print(f"table {tokens[1]} droped.")
            else:
                print(help["universe_adjust_drop"])
        elif tokens[0] == "-adjust_add":
            if len(tokens) == 2 and tokens[1]:
                if confirm() is None:
                    return
                controller.adjustTable([tokens[1]], "add")
                print(f"table {tokens[1]} added.")
            else:
                print(help["universe_adjust_add"])

        elif tokens[0] == "-adjust_replace":
            if len(tokens) == 2 and tokens[1]:
                if confirm() is None:
                    return
                controller.adjustTable([tokens[1]], "replace")
                print(f"table {tokens[1]} replaced.")
            else:
                print(help["universe_adjust_replace"])
        elif tokens[0] == "-adjust_offsets":
            if len(tokens) == 4:
                if confirm() is None:
                    return
                topic = tokens[1]
                partitions = tokens[2]
                offset = tokens[3]
                controller.adjustOffsetTable(topic, partitions, offset)
                print(f"table {tokens[1]} {tokens[2]} {tokens[3]}updated.")
            else:
                print(help["universe_adjust_offsets"])

        else:
            print(help["show_all"])

    except AttributeError as ex:
        print(f"{Style.DIM}Warning: {Style.RESET_ALL} {ex}")
    except Exception as e:
        print(f"{Style.DIM}Warning: {Style.RESET_ALL} {e}")


def load_ipython_extension(ipython, *args):
    def universe_completers(self, event):
        return [
        # "-table_list",
            "-describe",
        # "-recover",
            "-adjust_drop",
            "-adjust_add",
            "-adjust_replace",
            "-adjust_offsets",
        ]

    ipython.register_magic_function(dispatcher, "line", magic_name="universe")
    ipython.set_hook(
        "complete_command", universe_completers, str_key="universe")
    ipython.set_hook(
        "complete_command", universe_completers, str_key="%universe")


def unload_ipython_extension(ipython):
    pass
