import json
import sys
import re
import pandas as pd
import pprint
import traceback
from xcalar.solutions.tools.connector import XShell_Connector
from xcalar.solutions.analyzer import Analyzer
from tabulate import tabulate
from colorama import Style, Fore
from .action_confirm import confirm
pp = pprint.PrettyPrinter(indent=1, width=80, compact=False)
'''
Fore: BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE, RESET.
Back: BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE, RESET.
Style: DIM, NORMAL, BRIGHT, RESET_ALL
'''
def dispatcher(line):
    help = {
        'all_usage': f'''
Usage:
TABLE
    {Style.NORMAL}table {Style.DIM}-list{Style.RESET_ALL}                                                       {Style.BRIGHT}- lists all tables in the session relating to the controller including alias name, session name,
                                                                        table status (watermark/garbaged/dirty), number of records, size, skew, and is_pin.{Style.RESET_ALL}
    {Style.NORMAL}table {Style.DIM}-list_all{Style.RESET_ALL}                                                   {Style.BRIGHT}- lists all tables in the session including alias name, session name,
                                                                        table status (watermark/garbaged/dirty), number of records, size, skew, and is_pin.{Style.RESET_ALL}
    {Style.NORMAL}table {Style.DIM}-schema         <table_alias>{Style.RESET_ALL}                               {Style.BRIGHT}- shows table’s schema and primary keys.{Style.RESET_ALL}
    {Style.NORMAL}table {Style.DIM}-describe       <table_alias>{Style.RESET_ALL}                               {Style.BRIGHT}- shows table’s definition in universe, including metadata, sources and sinks.{Style.RESET_ALL}
    {Style.NORMAL}table {Style.DIM}-unpin          <session_table_name>{Style.RESET_ALL}                        {Style.BRIGHT}- unpins a table.{Style.RESET_ALL}
    {Style.NORMAL}table {Style.DIM}-pin            <session_table_name>{Style.RESET_ALL}                        {Style.BRIGHT}- pins a table.{Style.RESET_ALL}
    {Style.NORMAL}table {Style.DIM}-profile        <table_alias> <col_1, col_2>{Style.RESET_ALL}                {Style.BRIGHT}- profiles a table or a column if specified.{Style.RESET_ALL}
    {Style.NORMAL}table {Style.DIM}-record_show    <table_alias> <x:y>{Style.RESET_ALL}                         {Style.BRIGHT}- shows top 10 rows/[x:y rows] in the table.{Style.RESET_ALL}
        ''',
        'table_list': f'''
Usage:
    TABLES
    {Style.NORMAL}table {Style.DIM}-list           <session_name>{Style.RESET_ALL}                  {Style.BRIGHT}- lists all tables in the session relating to the controller including alias name, session name,
                                                            table status (watermark/garbaged/dirty), number of records, size, skew, and is_pin.{Style.RESET_ALL}
        ''',
        'table_list_all': f'''
Usage:
    TABLES
    {Style.NORMAL}table {Style.DIM}-list_all           <session_name>{Style.RESET_ALL}                  {Style.BRIGHT}- lists all tables in the session, including alias name, session name,
                                                            table status (watermark/garbaged/dirty), number of records, size, skew, and is_pin.{Style.RESET_ALL}
        ''',
        'table_describe': f'''
Usage:
    TABLE
    {Style.NORMAL}table {Style.DIM}-describe           <table_alias>{Style.RESET_ALL}               {Style.BRIGHT}- shows table’s definition in universe, including metadata, sources and sinks.{Style.RESET_ALL}
        ''',
        'table_drop': f'''
Usage:
    TABLE
        {Style.NORMAL}table {Style.DIM}-drop         <table_alias>{Style.RESET_ALL}                 {Style.BRIGHT}- drop table by table_session_name.{Style.RESET_ALL}
        ''',
        'table_profile': f'''
Usage:
    TABLE
    {Style.NORMAL}table {Style.DIM}-profile        <table_alias> <col_1, col_2>{Style.RESET_ALL}   {Style.BRIGHT}- profiles a table or a column if specified.{Style.RESET_ALL}
        ''',
        'table_pin': f'''
Usage:
    TABLE
    {Style.NORMAL}table {Style.DIM}-pin            <session_table_name>{Style.RESET_ALL}             {Style.BRIGHT}- pins a table.{Style.RESET_ALL}
    ex: table -profile ds1
        table -profile ds1 COL2,COL3
        ''',
        'table_unpin': f'''
Usage:
    TABLE
    {Style.NORMAL}table {Style.DIM}-unpin          <session_table_name>{Style.RESET_ALL}             {Style.BRIGHT}- unpins a table.{Style.RESET_ALL}
        ''',
        'table_record_delete': f'''
Usage:
    TABLE
        {Style.NORMAL}table {Style.DIM}-record_delete <table_alias> <x:y>{Style.RESET_ALL}          {Style.BRIGHT}- delete records at [x:y rows] in the table.{Style.RESET_ALL}
        ''',
        'table_record_insert': f'''
Usage:
    TABLE
        {Style.NORMAL}table {Style.DIM}-record_insert <table_alias> <x:y>{Style.RESET_ALL}           {Style.BRIGHT}- insert records at [x:y rows] in the table.{Style.RESET_ALL}
        ''',
        'table_record_update': f'''
Usage:
    TABLE
        {Style.NORMAL}table {Style.DIM}-record_update <table_alias>{Style.RESET_ALL}                 {Style.BRIGHT}- {Style.RESET_ALL}
        ''',
        'table_schema': f'''
Usage:
    TABLE
        {Style.NORMAL}table {Style.DIM}-schema         <table_alias>{Style.RESET_ALL}                 {Style.BRIGHT}- shows table’s schema and primary keys.{Style.RESET_ALL}
        ''',
    }
    # -----------------------
    # get user input
    # -----------------------
    tokens = line.split()
    if len(tokens) == 0:
        print(help['all_usage'])
        return
    try:
        # ---------------------
        # connectivity check
        # ---------------------
        if not (
                XShell_Connector.self_check_client()
                and XShell_Connector.self_check_controller()
        ):
            return None
        client = XShell_Connector.get_client()
        orchestrator = XShell_Connector.get_orchestrator(
            XShell_Connector.get_universe_id()
        )
        analyzer = XShell_Connector.get_analyzer()
        # if analyzer is None:
        #     print(f'Please run "{Fore.YELLOW}materialize{Style.RESET_ALL}".')
        #     return
        if tokens[0] == '-list_all':
            client = XShell_Connector.get_client()
            tabs = client.list_tables(session_name_pattern='*')
            # tabs = client.list_tables(
            #     session_name_pattern=XShell_Connector.get_universe_id()
            # )
            cols = [
                # 'id',
                'name',
                'session_name',
                'session_id',
                'size',
                'skew',
                'rows',
                'is_pinned',
                'is_shared',
            ]
            data = []

            for tab in tabs:
                temp = []
                meta = tab._get_meta()
                name = meta.table_name
                size = meta.total_size_in_bytes
                skew = meta.node_skew()
                rows = meta.total_records_count
                is_pinned = tab.is_pinned()
                is_shared = tab.shared
                temp.append(name)
                temp.append(tab.session.name)
                temp.append(tab.session.session_id)
                temp.append(size)
                temp.append(skew)
                temp.append(rows)
                temp.append(is_pinned)
                temp.append(is_shared)
                data.append(temp)

            print(
                tabulate(
                    pd.DataFrame.from_records(data, columns=cols).sort_values(by=['name']),
                    headers='keys',
                    tablefmt='psql',
                )
            )
        elif tokens[0] == '-list':
            client = XShell_Connector.get_client()
            tabs = client.list_tables(
                session_name_pattern=XShell_Connector.get_universe_id()
            )
            cols = [
                'id',
                'alias',
                'name',
                'size',
                'skew',
                'rows',
                'is_pinned',
                'is_shared',
            ]
            data = []
            if orchestrator is None:
                # PYTHONPATH may not set correctly.
                # ex:
                # "PYTHONPATH": "/home/${env:USER}/xcalar/src/bin/tests/controllerTest"
                print(f'{Style.DIM}Warning: {Style.RESET_ALL} orchestrator is null !')
                return

            watermark_tables_dict = {value: key for (key, value) in orchestrator.watermark.mapObjects().items() if value is not None}

            for tab in tabs:
                temp = []
                meta = tab._get_meta()
                alias = watermark_tables_dict.get(meta.table_name, '-')
                is_commited = (tab.name in watermark_tables_dict)
                id = meta.table_id
                name = meta.table_name
                size = meta.total_size_in_bytes
                skew = meta.node_skew()
                rows = meta.total_records_count
                is_pinned = tab.is_pinned()
                is_shared = tab.shared
                temp.append(id)
                temp.append(alias)
                temp.append(name)
                temp.append(size)
                temp.append(skew)
                temp.append(rows)
                temp.append(is_pinned)
                temp.append(is_shared)
                data.append(temp)

            print(
                tabulate(
                    pd.DataFrame.from_records(data, columns=cols).sort_values(by=['name']),
                    headers='keys',
                    tablefmt='psql'
                )
            )
        elif tokens[0] == '-describe':
            if len(tokens) == 2 and tokens[1]:
                table_name = tokens[1]
                flat_universe = XShell_Connector.get_flat_universe()
                if table_name not in flat_universe.keys():
                    print(
                        f'{Style.DIM}Warning: {Style.RESET_ALL} {table_name} not found!'
                    )
                    return
                print(json.dumps(flat_universe[table_name], indent=1))
            else:
                print(help['table_describe'])
        elif tokens[0] == '-drop':
            if len(tokens) == 2:
                if confirm() is False:
                    return
                droped_table_name = tokens[1]
                uber_dispatcher = XShell_Connector.get_uber_dispatcher()
                uber_dispatcher.dropTable(droped_table_name)
                droped_list = []
                tabs = client.list_tables()
                for tab in tabs:
                    if droped_table_name == tab.name:
                        tab.unpin()
                        tab.drop()
                        droped_list.append(tab.name)
                if len(droped_list) > 0:
                    print(
                        f'{Style.DIM}Warning: {Style.RESET_ALL} {droped_table_name} dropped!'
                    )
                else:
                    print(
                        f'{Style.DIM}Warning: {Style.RESET_ALL} {droped_table_name} does not exists!'
                    )
        elif tokens[0] == '-schema':
            if len(tokens) == 2 and tokens[1]:
                table_name = tokens[1]
                flat_universe = XShell_Connector.get_flat_universe()
                if table_name not in flat_universe.keys():
                    print(
                        f'{Style.DIM}Warning: {Style.RESET_ALL} {table_name} not found!'
                    )
                    return
                data = flat_universe[table_name]
                for mykey in data.keys():
                    if data[mykey] is None:
                        continue
                    dict_keys = data[mykey].keys()
                    dict_values = data[mykey].values()
                    t = zip(dict_keys, dict_values)
                    if mykey in ['schema', 'pks']:
                        print(
                            f'\n{mykey}:\n',
                            tabulate(t, headers=['name', 'type'], tablefmt='psql', ),
                        )
            else:
                print(help['table_schema'])
        elif len(tokens) >= 1 and tokens[0] == '-pin':
            if len(tokens) == 2 and tokens[1] == 'all':
                if confirm() is False:
                    return
                tables = ''
                for t in orchestrator.dispatcher.session.list_tables():
                    if not t.is_pinned():
                        tables = tables + ', ' + t
                        t.pin()
                print(f'pin {tables} successfully')
            elif len(tokens) == 2:
                if confirm() is False:
                    return
                table = orchestrator.dispatcher.session.get_table(tokens[1])
                table.pin()
                print(f'pin {tokens[1]} successfully!')
            else:
                print(help['table_pin'])
        elif len(tokens) == 2 and tokens[0] == '-unpin':
            if tokens[1] == 'all':
                if confirm() is False:
                    return
                tables = ''
                for t in orchestrator.dispatcher.session.list_tables():
                    if t.is_pinned():
                        tables = tables + ', ' + t
                        t.unpin()
                print(f'unpin {tables} successfully!')
            elif tokens[1]:
                if confirm() is False:
                    return
                table = orchestrator.dispatcher.session.get_table(tokens[1])
                table.unpin()
                print(f'unpin {tokens[1]} successfully!')
            else:
                print(help['table_unpin'])
        elif tokens[0] == '-profile':
            if len(tokens) >= 3 and tokens[1]:
                table_name = tokens[1]
                col_names = ''.join(tokens[2:]).strip().split(',')
                for col_name in col_names:
                    df = analyzer.profile(table_name, col_name)
                    df.index.name = 'idx'
                    df.showindex = False
                    print(
                        tabulate(
                            df,
                            headers='keys',
                            tablefmt='psql',
                        )
                    )
            else:
                print(help['table_profile'])
        else:
            print(help['all_usage'])
    except KeyboardInterrupt:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
        return
    except Exception as err:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {err}')
        traceback.print_tb(err.__traceback__)
def load_ipython_extension(ipython, *args):
    def table_completers(self, event):
        return [
            '-describe',
            '-drop',
            '-list',
            '-list_all',
            '-pin',
            '-profile',
            '-schema',
            '-status',
            '-unpin',
        ]
    ipython.register_magic_function(dispatcher, 'line', magic_name='table')
    ipython.set_hook('complete_command', table_completers, str_key='table')
    ipython.set_hook('complete_command', table_completers, str_key='%table')
def unload_ipython_extension(ipython):
    # del ipython.magics_manager.magics['table']['xyz']
    pass
