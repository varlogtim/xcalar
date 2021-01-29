# nomart_ext.py

from colorama import Style
from xcalar.solutions.tools.connector import XShell_Connector
from xcalar.solutions.query_builder import QueryBuilder
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.solutions.tools.arg_parser import ArgParser
from pathlib import Path
import os
import traceback

argp = ArgParser('nomart', [
    {
        'verbs': {
            'test': 'test'
        },
        'desc': 'Refresh LogMart tables from the cluster logs.'
    },
])


def dispatcher(line):
    usage = argp.get_usage()
    input_params = argp.get_params(line)
    mart_name = 'JIRA MART'

    try:
        xshell_connector = XShell_Connector()
        client = xshell_connector.get_client()
        dispatcher = xshell_connector.get_uber_dispatcher()

        if 'test' in input_params:
            targetName = 'generated'
            dispatcher.update_or_create_datatarget(targetName, 'memory')
        else:
            print(usage)
            return

        # params = {'start_ts': '2020-03-28T21:00:00.000Z', 'end_ts': '9999-01-01T00:00:00.000Z'} # 'a_start_ts': '2020-03-28T21:00:00.000Z',
        params = {
            'stack_size': 20,
            'start_ts': '2000-01-01T21:00:00.000Z',
            'end_ts': '9999-01-01T00:00:00.000Z'
        }
        for k, v in params.items():
            if k in input_params:
                params[k] = input_params[k]
            else:
                print(f'-> {k} not provided, setting to {v}')

        schema_jql = {
            "xpu": "integer",
            "startAt": "integer",
            "maxResults": "integer",
            "total": "integer",
            "key": "string",
            "project": "string",
            "issuetype": "string",
            "customers": "string",
            "sprint": "string",
            "labels": "string",
            "components": "string",
            "customfield_10014": "string",
            "fixVersions": "string",
            "lastViewed": "timestamp",
            "created": "timestamp",
            "priority": "string",
            "updated": "timestamp",
            "status": "string",
            "description": "string",
            "summary": "string",
            "assignee": "string",
            "creator": "string",
            "reporter": "string"
        }

        s_table = load_source_table(dispatcher, client, 'jira_parser',
                                    schema_jql, targetName, '50', '', 'jira')
        publish_table(dispatcher, s_table, 'JIRAMART')
        dispatcher.unpinDropTables(prefix='ut_jira_*')

        print(f'-> Sucessfully created published tables in {mart_name}')

    # except AttributeError as ex:
    #     print(f'{Style.DIM}Warning: {Style.RESET_ALL} {ex}')
    except KeyboardInterrupt:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
        return
    except Exception as err:
        # print(f'{Style.DIM}Warning: {Style.RESET_ALL} {e}')
        traceback.print_tb(err.__traceback__)


def build_table(dispatcher, sql, pks, params, prefix):
    prefix = f'logmart_{prefix}'
    x_table = execute_sql(dispatcher, sql, params, pks, prefix=prefix)
    publish_table(dispatcher, x_table, prefix.upper())


def load_source_table(dispatcher, client, udf_name, schema, targetName, path,
                      fileNamePattern, table_name):
    # fixme: replace with uber dispatcher method
    with open(
            os.path.join(
                os.path.dirname(Path(__file__).parents[1]) +
                f'/udf/{udf_name}.py')) as fp:
        dispatcher.update_or_create_udf(udf_name, fp.read(), unique_name=False)

    print(f'Parsing {table_name} Logs...')
    qb_all = QueryBuilder(schema=schema)
    qb_all.XcalarApiBulkLoad(targetName=targetName, path=path, fileNamePattern=fileNamePattern,
                             parserFnName=f'{udf_name}:parse', pk='row_num_pk', recursive=True) \
        .XcalarApiSynthesize(columns=qb_all.synthesizeColumns(fromFP=True))
    tmp_table_all = dispatcher.execute(
        qb_all, schema=qb_all.schema, prefix=table_name)
    records = tmp_table_all.record_count()
    if records == 0:
        raise Exception('No records loaded)')
    print(f'-> Parsed {records} {table_name} records')
    return tmp_table_all.name


def execute_sql(dispatcher, sql_query, params, pks=None, prefix='logmart_'):
    for p, v in params.items():
        sql_query = sql_query.replace(f'<{p}>', f'{v}')

    tbl_name = dispatcher.session.execute_sql(
        sql_query=sql_query,
        result_table_name=dispatcher.nextTable(prefix='logmart_'))
    if pks:
        table = dispatcher.get_table(tbl_name)
        schema = table.schema
        qb = QueryBuilder(schema=schema)
        qb.XcalarApiSynthesize(table_in=tbl_name, columns=qb.synthesizeColumns(), sameSession=False) \
          .XcalarApiIndex(keys={pk: schema[pk] for pk in pks})
        tbl = dispatcher.execute(qb, desc='indexing', prefix=prefix)
        return tbl.name
    else:
        return tbl_name


def publish_table(dispatcher, session_table_name, pub_table_name):
    op = Operators(dispatcher.xcalarApi)
    verb = 'Published'
    try:
        op.unpublish(pub_table_name)
        # print(f'Deleted previously published {pub_table_name} table')
        verb = 'Re-published'
    except Exception as e:
        if 'Publish table name not found' in str(e):
            pass
    op.publish(session_table_name, pub_table_name)
    print(f'-> {verb} {pub_table_name} table')


def load_ipython_extension(ipython, *args):
    argp.register_magic_function(ipython, dispatcher)


def unload_ipython_extension(ipython):
    pass
