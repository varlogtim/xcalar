# SF-482
import json
import pandas as pd

# from xcalar.solutions.singleton import Singleton_Controller
from xcalar.solutions.tools.connector import XShell_Connector
from colorama import Style
from tabulate import tabulate
from pygments import highlight
from pygments.lexers import JsonLexer
from pygments.formatters import TerminalFormatter
from .action_confirm import confirm


def dispatcher(line):
    help = {
        'show_all':
            f"""
Usage:

DATASET
    {Style.NORMAL}dataset {Style.DIM}-list{Style.RESET_ALL}                                                     {Style.BRIGHT}- lists all datasets in xd.{Style.RESET_ALL}
    {Style.NORMAL}dataset {Style.DIM}-show      <dataset> <#start : #rows>{Style.RESET_ALL}                     {Style.BRIGHT}- shows top 10 rows/[x:y rows] in the dataset.{Style.RESET_ALL}
    {Style.NORMAL}dataset {Style.DIM}-describe  <dataset>{Style.RESET_ALL}                                      {Style.BRIGHT}- shows information of a dataset, including number of records, etc.{Style.RESET_ALL}
    {Style.NORMAL}dataset {Style.DIM}-drop      <dataset>{Style.RESET_ALL}                                      {Style.BRIGHT}- deletes the dataset.{Style.RESET_ALL}
""",
        'dataset_list':
            f"""
Usage:
    dataset
    {Style.NORMAL}dataset {Style.DIM}-list{Style.RESET_ALL}                       {Style.BRIGHT}- lists all datasets in xd.{Style.RESET_ALL}
        """,
        'dataset_show':
            f"""
Usage:
    dataset
    {Style.NORMAL}dataset {Style.DIM}-show <dataset>{Style.RESET_ALL} <#start : #rows>            {Style.BRIGHT}- shows top 10 rows/[x:y rows] in the dataset.{Style.RESET_ALL}
        """,
        'dataset_describe':
            f"""
Usage:
    dataset
    {Style.NORMAL}dataset {Style.DIM}-describe <dataset>{Style.RESET_ALL}         {Style.BRIGHT}- shows information of a dataset, including number of records, etc.{Style.RESET_ALL}

        """,
        'dataset_drop':
            f"""
Usage:
    dataset
        {Style.NORMAL}dataset {Style.DIM}-drop <dataset>{Style.RESET_ALL}             {Style.BRIGHT}- deletes the dataset.{Style.RESET_ALL}
        """,
    }

    def show_dataset(dataset_name, start, num_rows):
        dataset = controller.orchestrator.dispatcher.xcalarClient.get_dataset(
            dataset_name)
        columns = list(map(lambda x: x['name'], dataset.get_info()['columns']))
        show_table_or_dataset(dataset, columns, start, num_rows)

    def show_table_or_dataset(table_or_dataset, columns, start, num_rows):
        data = []
        for row in table_or_dataset.records(
                start_row=start, num_rows=num_rows):
            r_data = []
            for col in columns:
                if col in row:
                    r_data.append(row[col])
                else:
                    r_data.append(None)
            data.append(tuple(r_data))
        print(
            tabulate(
                pd.DataFrame.from_records(data, columns=columns),
                showindex=range(start, start + len(data)),
                headers='keys',
                tablefmt='psql',
            ))

    tokens = line.split()

    if len(tokens) == 0:
        print(help['show_all'])
        return

    try:
        # singleton_controller = Singleton_Controller.getInstance()
        # controller = singleton_controller.get_controller()
        xshell_connector = XShell_Connector()
        controller = xshell_connector.get_controller()
        # dataset
        if tokens[0] == '-list':
            datasets = controller.orchestrator.dispatcher.xcalarClient.list_datasets(
            )
            names = list(map(lambda x: x.name, datasets))

            t = zip(names)
            print(tabulate(
                t,
                headers=['dataset'],
                tablefmt='psql',
            ))

        elif tokens[0] == '-show':
            if len(tokens) == 3 and tokens[1]:
                dataset_name = tokens[1]
                x = int(tokens[2].split(':')[0])
                y = int(tokens[2].split(':')[1])

                show_dataset(dataset_name, x, y)
            else:
                print(help['dataset_show'])

        elif tokens[0] == '-describe':
            if len(tokens) == 2 and tokens[1]:
                dataset = controller.orchestrator.dispatcher.xcalarClient.get_dataset(
                    tokens[1])
                info = dataset.get_info()

                json_object = json.loads(json.dumps(info))
                json_str = json.dumps(json_object, indent=4, sort_keys=True)
                print(highlight(json_str, JsonLexer(), TerminalFormatter()))

            else:
                print(help['dataset_describe'])

        elif tokens[0] == '-drop':
            if len(tokens) == 2 and tokens[1]:
                if confirm() is None:
                    return
                pass
            else:
                print(help['dataset_drop'])

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
    def dataset_completers(self, event):
        return [
            '-list',
            '-show',
            '-describe',
            '-drop',
        ]

    ipython.register_magic_function(dispatcher, 'line', magic_name='dataset')
    ipython.set_hook('complete_command', dataset_completers, str_key='dataset')
    ipython.set_hook(
        'complete_command', dataset_completers, str_key='%dataset')


def unload_ipython_extension(ipython):
    pass
