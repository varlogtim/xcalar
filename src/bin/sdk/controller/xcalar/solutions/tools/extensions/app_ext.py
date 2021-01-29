# app_ext
from tabulate import tabulate
from colorama import Style
# import pandas as pd
import json
import traceback
import concurrent.futures
import time
import re

from xcalar.solutions.tools.arg_parser import ArgParser
from xcalar.solutions.tools.connector import XShell_Connector
from xcalar.solutions.query_builder import QueryBuilder
from xcalar.solutions.git.git_api import GitApi
from xcalar.solutions.sdlc import SdlcManager

# from xcalar.solutions.properties_adapter import PropertiesAdapter

reverseMap = {
    'DfInt64': 'integer',
    'DfUInt32': 'integer',
    'DfFloat32': 'float',
    'DfFloat64': 'float',
    'DfString': 'string',
    'DfBoolean': 'boolean',
    'DfTimespec': 'timestamp',
    'DfMoney': 'money'
}

argp = ArgParser('app', [
    {
        'verbs': {
            'test': 'path to test properties file', #'/home/nikita/test/runtime_refprops.json'
            'threads_num': 'number of tests to run in parallel',
            'threads_delay': 'staggering interval before starting next thread',
            'runs_num': 'number of executions in each thread',
            'runs_interval':'pause between runs within each thread'
        },
        'desc': 'Perf-test applciations'
    },
    {
        'verbs': {
            'test_plan': '<plan name>', 
            'args': 'JSON dictionary of parameters',
            'tname': 'output table name',
            # 'threads_num': 'number of tests to run in parallel',
            # 'threads_delay': 'staggering interval before starting next thread',
            # 'runs_num': 'number of executions in each thread',
            # 'runs_interval':'pause between runs within each thread'
        },
        'desc': 'Unit test app'
    },
    {
        'verbs': {
            'load': '<target name>:<path> (1 for memory targets)', #'/home/nikita/test/runtime_refprops.json'
            'parser': '<(/sharedUDFs/)module:function>',
            'args': 'JSON dictionary of parameters',
            'tname': 'output table name'
        },
        'desc': 'Test bulk-load'
    },
    {
        'verbs': {
            'checkin': '<plan name>', 
            'properties': '<path to gitlab prop file>'
        },
        'desc': 'check-in'
    },
])


# cat /home/nikita/test/runtime_refprops.json
# {
#   "name": "ds2_counts",
#   "source": {
#     "driver": "git.git_api:GitCheckinUtil",
#     "args": {
#       "url": "https://gitlab.com/api/v4",
#       "projectId": "14164315",
#       "accessToken": "jALRsUHgQ2EC52WSg3Wv",
#       "branchName": "test",
#       "path": "nikita/modular_refiner2/app_test"
#     }
#   },
#   "tables": {
#     "ds1": "ut_ds1_base1598588304#351921",
#     "ds1_delta": "ut_uber1598588341#200896"
#   },
#   "runtimeParams": {
#     "max8": 10,
#     "system": "test",
#     "test": 100,
#     "batch_id": 12
#   }
# }

def dispatcher(line):
    usage = argp.get_usage()
    input_params = argp.get_params(line)

    try:
        if 'test' in input_params:
            params = argp.init_params(
                input_params, defaults={
                    'threads_num': 1,
                    'threads_delay': 2,
                    'runs_num': 2,
                    'runs_interval': 2,
                    'test': '',
                })
            refinerTestHarness(params)
        elif 'test_plan' in input_params:
            params = argp.init_params(
                input_params, defaults={
                    # 'threads_num': 1,
                    # 'threads_delay': 2,
                    # 'runs_num': 2,
                    # 'runs_interval': 2,
                    'args': '{}',
                    'test_plan': '',
                    'tname': 'compute#123'

                })
            test_plan(params)
        elif 'load' in input_params:
            params = argp.init_params(
                input_params, defaults={
                    'load': '',
                    'args': '{}',
                    'parser': '',
                    'schema': '{}',
                    'tname': None
                })
            loadTestHarness(params)
        elif 'checkin' in input_params:
            params = argp.init_params(
                input_params, defaults={
                    'properties': '',
                    'checkin': '',
                })
            checkin(params)
        else:
            print(usage)
            return
    except KeyboardInterrupt:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
        return
    except Exception as err:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {err}')
        traceback.print_tb(err.__traceback__)




def checkin(params):
    plan_name = params['checkin']
    # git_args = json.load(params['properties'])
    with open(params['properties'], 'r') as f:
        git_args = json.load(f)

# =========
# SDLC
#     {Style.NORMAL}sdlc {Style.DIM}-checkin  <git_branch> <git_path> <workbook> [ <dataflow> ... ]{Style.RESET_ALL}   {Style.BRIGHT}- checkin optimized dataflows with dependent interactive dataflows to git\n{Style.RESET_ALL}
#     {Style.NORMAL}sdlc {Style.DIM}-checkout <git_branch> <git_path>                             {Style.RESET_ALL}   {Style.BRIGHT}- interactive dataflows in a git branch and path.{Style.RESET_ALL}
#     """
    oz = XShell_Connector()
    disp = oz.get_uber_dispatcher()

    branch_name = git_args['branchName']
    path = git_args['path']
    gitApi = GitApi(git_args['url'], git_args['projectId'], 
        git_args['accessToken'])
    sdlcManager = SdlcManager(disp, gitApi)

    workbook_name = disp.workbook.name
    sdlcManager.checkin_all_dataflows([plan_name],
                                branch_name,
                                path,
                                workbook_name=workbook_name)

def loadTestHarness(params):
    targetName, path = params['load'].split(':')
    parserFnName = params['parser']
    parseArgs = json.loads(params['args'])
    schema = json.loads(params['schema'])
    tname = params['tname']
    # if schema is not json: try loading

    # schema = {"SYMBOL":"string", "DESCRIPTION":"string", "EXCHANGE":"string"}

    oz = XShell_Connector()
    disp = oz.get_uber_dispatcher()

    try:
        qb = QueryBuilder(schema)
        # fromFP = True
        tb_name = qb.XcalarApiBulkLoad(parserArg=parseArgs,
                                    targetName=targetName,
                                    parserFnName=parserFnName,
                                    path=path,
                                    recursive=False) \
            .XcalarApiSynthesize(columns=qb.synthesizeColumns(upperCase=True, fromFP=True))
        if tname:
            disp.unpinDropTables(prefix=tname)
        table = disp.execute(
            qb, prefix=f'test', pin_results=True, forced_name=tname)

        # with open(properties_file_path, 'r') as f:
        #     props = json.load(f)
    except Exception as e:
        print(f'{Style.DIM}Error loading: {Style.RESET_ALL} {e}')
        traceback.print_tb(e.__traceback__)



def test_plan(params):
    plan_name = params['test_plan']
    tname = params['tname']
    args = json.loads(params['args'])

    # threads_num = int(params['threads_num'])
    # threads_delay = int(params['threads_delay'])
    # runs_num = int(params['runs_num'])
    # runs_interval = int(params['runs_interval'])
    
    oz = XShell_Connector()
    disp = oz.get_uber_dispatcher()
    if tname:
        disp.unpinDropTables(prefix=tname)
    try:
        t = disp.get_table(args['SOURCE'])
        print(f'>>> Input rows: {t.record_count()}')
    except Exception as e:
        print(f'{Style.DIM}Error loading properties: {Style.RESET_ALL} {e}')
    # qs, optimized_query = disp.getDataflow(plan_name)
    disp.executeDataflow(plan_name, params=args, forced_name=tname)

def refinerTestHarness(params):
    properties_file_path = params['test']
    threads_num = int(params['threads_num'])
    threads_delay = int(params['threads_delay'])
    runs_num = int(params['runs_num'])
    runs_interval = int(params['runs_interval'])
    oz = XShell_Connector()
    disp = oz.get_uber_dispatcher()

    try:
        with open(properties_file_path, 'r') as f:
            props = json.load(f)
    except Exception as e:
        print(f'{Style.DIM}Error loading properties: {Style.RESET_ALL} {e}')

    git_args = props['source']['args']
    gitApi = GitApi(git_args['url'], git_args['projectId'], 
         git_args['accessToken'])
    sdlcManager = SdlcManager(disp, gitApi)
    df = sdlcManager.checkout_dataflow( git_args['branchName'], 
        git_args['path'], props['name'])
    tables = re.findall(r'"source": "<([_a-zA-Z0-9]*)>"', df.query_string)
    print(f'Expected input tables: {tables}')

    if len(tables) == 0:
        raise Exception(f'Application {name} has no inputs')

    print(f'Running {props["name"]} test with {threads_num} threads')
    worker_args = [(i, disp, props, df, threads_delay, runs_num, runs_interval) for i in range(1, threads_num + 1)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads_num) as executor:
        results = executor.map(
            lambda args: test_refiner(*args), worker_args)
        for ret in results:
            assert ret == 0

def test_refiner(i, disp, props, df, delay_sec, runs_num, runs_interval):
    time.sleep(delay_sec * i)
    print(f'Executing {props["name"]} iteration {i}...')
    params = {**props['tables'], **props['runtimeParams']}

    for j in range(1, runs_num + 1):
        try:
            delta_table = disp.executeDataflow(
                df, params=params, prefix=f'{props["name"]}_{i}_thread')
            record_count = delta_table.record_count()
            print(f'Reultset: {record_count} droping ...')
            disp.dropTable(delta_table)
        except Exception as e:
            print(f'{Style.DIM}Error executing: {Style.RESET_ALL} {e}')
        time.sleep(runs_interval)
    return 0

def load_ipython_extension(ipython, *args):
    argp.register_magic_function(ipython, dispatcher)

def unload_ipython_extension(ipython):
    pass
