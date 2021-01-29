# clr_mart_ext.py

import os
from colorama import Style
from pathlib import Path

from xcalar.external.LegacyApi.Operators import Operators
from xcalar.solutions.tools.connector import XShell_Connector
from xcalar.solutions.query_builder import QueryBuilder
from xcalar.solutions.tools.arg_parser import ArgParser
import traceback

argp = ArgParser('clr_mart', [
    {
        'verbs': {
            'refresh': 'log_root'
        },
        'desc': 'Refresh Controller Mart tables from the cluster logs.'
    },
    {
        'verbs': {
            'load_asup': 'asup_path'
        },
        'desc': 'Load Controller Mart tables from the specified ASUP location.'
    },
    {
        'verbs': {
            'load_logs': 'logs_path',
            'target': 'target_name'

        },
        'desc':
            'Load Controller Mart tables from the specified untarred location.'
    },
])

SQL_PARSER = """with tt as (select
 case when message like '%__EVENT: UNIVERSE LOAD STARTING%' then 1
      when message like '%__EVENT: STARTING SNAPSHOT RECOVERY%' then -1
      else cast(sxdf_cut(message,2,'__EVENT: BATCH START, batch id ') as int)
      end as Batch_ID,
 TIMESTAMP,
 cast(sxdf_cut(message,2,'Interval: ') as float) as _INTERVAL,
 universe_id as UNIVERSE,
 substring_index(sxdf_cut(message, 2, '__EVENT: '),',',1) as event,
 sxdf_cut(sxdf_cut(message, 2, 'ut_'),1,',') as OBJECT1,
 case when substr(message, 1,6)='State:' then
   substring_index(message, 'state: ', 1)
   else null end as state,
 message as MSG, row_num_pk
from <t>)
select
 Batch_ID, TIMESTAMP, _INTERVAL as INTERVAL,  UNIVERSE,  OBJECT1, STATE,
 case when trim(substring_index(event, ' ', 1)) = 'EXPORT' then
   substring_index(event, ' ', 2)
   when state = 'Snapshotting orchestrator' then 'SNAPSHOTTING'
   when MSG like '%Starting%snapshots%' then 'SNAPSHOTTING'
   when state = 'Snapshot finished successfully' then 'SNAPSHOT DONE'
   else event end as event, MSG,
 --cast('2020-03-20T18:18:18.123' as TIMESTAMP) as tes,
 ROW_NUMBER() OVER(PARTITION by UNIVERSE ORDER BY TIMESTAMP,row_num_pk ASC) AS rn
 from tt
where substring_index(MSG, 'unpinning', 1)<>''
 and substring_index(MSG, ' ', 2) not in ('State: Sleeping', 'State: Next', 'State: Current')
 and timestamp BETWEEN cast('<start_ts>' as timestamp) and cast('<end_ts>' as timestamp)"""
SQL_GET_BATCHID = """with b as (
SELECT rn,
  BATCH_ID,
  UNIVERSE,
  TIMESTAMP as batch_start_ts,
  1-ROW_NUMBER() OVER(PARTITION by BATCH_ID, UNIVERSE ORDER BY TIMESTAMP DESC) AS v
  from <t>
  where BATCH_ID>-9999
  ), bb as (
SELECT   b1.rn,
coalesce(min(b2.rn),100000000) as rn_next,
  b1.BATCH_ID as batch_id,
  b1.batch_start_ts,
  b1.UNIVERSE,
  b1.v as batch_version
from b b1 left join b b2
  on b2.rn>b1.rn and b2.UNIVERSE=b1.UNIVERSE
group by b1.rn, b1.BATCH_ID, b1.UNIVERSE, b1.batch_start_ts, b1.v),
bbb as (
SELECT   b1.rn, b1.rn_next-1 as rn_next, --create interval
--attribute snapshots to the next batch_id
  case when b1.batch_id >= 0 then b1.batch_id else b2.batch_id end as batch_id,
  b1.batch_start_ts,
  b1.UNIVERSE,
  b1.batch_version
from bb b1 left join bb b2 on b2.UNIVERSE=b1.UNIVERSE and b1.rn_next = b2.rn)
select UNIVERSE,BATCH_VERSION,BATCH_ID,
concat(BATCH_ID,'-',batch_version,'-',UNIVERSE) as batch_universe_id,
min(RN) as rn,
max(RN_NEXT) as rn_next,
min(BATCH_START_TS) as BATCH_START_TS,
count(*) as cnt
from bbb
group by 1,2,3,4"""
SQL_GET_STAGE = """with b as (
SELECT rn, EVENT, UNIVERSE from <t>
  where EVENT <> '')
SELECT   b1.rn,
coalesce(min(b2.rn),100000000)-1 as rn_next,
  b1.EVENT as EVENT,
  b1.UNIVERSE
  from b b1 left join b b2 on b2.rn>b1.rn and b2.UNIVERSE=b1.UNIVERSE
  group by b1.rn, b1.EVENT, b1.UNIVERSE"""
SQL_CONTROLLER = """select c.batch_id,
c.BATCH_START_TS,
c.batch_version as batch_version,
c.batch_universe_id,
t1.universe,
s.EVENT as event_span,
t1.TIMESTAMP,
coalesce(cast((unix_timestamp(t1.timestamp) - t1.INTERVAL)*1000 as TIMESTAMP), t2.timestamp)  as prev_timestamp,
coalesce(t1.INTERVAL, unix_timestamp(t1.timestamp)-unix_timestamp(t2.timestamp)) as duration,
t1.MSG,
t1.OBJECT1 as object,
case when t1.OBJECT1 like '%15%' then
substring_index(t1.OBJECT1, '15', 1)
end as object_type,
t2.msg as prev_msg,
concat(t1.universe,'-',t1.rn) as row_id
from <t> t1 left outer join <t> t2
on t1.rn = t2.rn + 1 and t1.universe=t2.universe
join <c> c on t1.rn BETWEEN c.rn and c.rn_next and c.universe=t1.universe
join <s> s on t1.rn BETWEEN s.rn and s.rn_next and s.universe=t1.universe"""
# WHERE coalesce(unix_timestamp(t1.timestamp) - t1.INTERVAL, unix_timestamp(t2.timestamp)) > unix_timestamp(cast('<a_start_ts>' as timestamp))
SQL_APPLICTIONS = """with a as (
select UNIVERSE,BATCH_ID, BATCH_VERSION, batch_universe_id, TIMESTAMP,
sxdf_cut(sxdf_cut(sxdf_cut(msg,2,' '),2,':'),1,'_delta') as source_delta,
case when msg like 'Prepared self%' then UNIVERSE
  else sxdf_cut(sxdf_cut(msg,2,' '),1,':')
  end as source_universe,
sxdf_cut(sxdf_cut(msg,4,' '),1,',') as application,
cast(sxdf_cut(sxdf_cut(msg,2,', '),2,': ') as int) as low_batch_id,
cast(sxdf_cut(sxdf_cut(msg,3,', '),2,': ') as int) as high_batch_id
from <c> c where msg like 'Prepared %'
)
select a.UNIVERSE, a.BATCH_ID, a.BATCH_VERSION, a.BATCH_UNIVERSE_ID, a.APPLICATION,
a.SOURCE_UNIVERSE, a.LOW_BATCH_ID, a.HIGH_BATCH_ID,
a.SOURCE_DELTA as SOURCE_DELTA_TABLE,
coalesce(sum(x.row_count),0) as ROW_COUNT, count(*) as batch_count
from a left join <x> x on a.SOURCE_UNIVERSE = x.UNIVERSE and a.SOURCE_DELTA = x.TABLE_NAME
and a.BATCH_VERSION=x.BATCH_VERSION and
x.BATCH_ID <=a.HIGH_BATCH_ID and x.BATCH_ID >= a.LOW_BATCH_ID
group by 1,2,3,4,5,6,7,8,9"""  # x, c

SQL_TABELS1 = """select UNIVERSE, BATCH_ID, BATCH_VERSION, batch_universe_id,
sxdf_cut(sxdf_cut(t.MSG,2,'Batch contains '),1,'}') as batch_info
from <t> t where t.MSG like 'State: Batch contains %'"""

SQL_STAGES = """with stages as (
SELECT t.UNIVERSE, t.BATCH_ID, t.BATCH_VERSION, t.batch_universe_id, t.EVENT_SPAN,
MAX(TIMESTAMP) as stage_end_ts,
MIN(PREV_TIMESTAMP) as stage_start_ts,
cast((unix_timestamp(MAX(TIMESTAMP))-unix_timestamp(MIN(PREV_TIMESTAMP)))/0.6 as int)/100 as stage_DURATION_mins
from <t> t group by 1,2,3,4,5),
timer as (
select t.UNIVERSE, t.BATCH_ID, t.BATCH_VERSION, t.batch_universe_id,
 t.EVENT_SPAN as stage, t.TIMESTAMP, t.PREV_TIMESTAMP,
 t.DURATION as DURATION_secs, t.OBJECT, t.OBJECT_TYPE,
 s.stage_start_ts, s.stage_end_ts, s.stage_DURATION_mins,
 cast (t.DURATION*1000 / s.stage_DURATION_mins / 6 as int) / 100 as pcnt_stage_duration,
 --sxdf_cut(sxdf_cut(sxdf_cut(OBJECT,1,'_delta'),1,'_base'),1,'15') as universe_table_name,
 sxdf_cut(sxdf_cut(sxdf_cut(sxdf_cut(OBJECT,1,'_delta'),1,'_base'),1,'_fromstream'),1,'15') as universe_table_name,
 sxdf_cut(msg,2,'Timer: ') as msg,
 sxdf_cut(sxdf_cut(msg,2,' dataflow '),1,',') as job_id,
 sxdf_cut(msg,2,' ') as type ,t.ROW_ID
from <t> t join stages s on s.batch_universe_id=t.batch_universe_id and s.EVENT_SPAN=t.EVENT_SPAN
where msg like 'Timer: %'),
timer_plus as (
SELECT ti.*,
 sxdf_cut(OBJECT_TYPE,2,universe_table_name) as _operation_type,
 case when UNIVERSE_TABLE_NAME in ('info', 'getDatasetCounts', 'getUberOffsets', 'offsets', 'offsets_commit', 'uber', 'analyzer', 'getKafkaOffsets') then 1
    else 0 end as IS_AUX
 from timer ti)
SELECT ti.UNIVERSE, ti.BATCH_ID, ti.BATCH_VERSION, ti.batch_universe_id,
 ti.stage as CONTROLLER_STAGE,
 ti.TIMESTAMP, ti.PREV_TIMESTAMP, ti.DURATION_secs as DURATION_SEC,
 ti.OBJECT_TYPE, ti.stage_start_ts, ti.stage_end_ts,
 ti.stage_DURATION_mins as STAGE_DURATION_MIN, ti.pcnt_stage_duration,
 ti.universe_table_name, ti.msg as LOG_MSG, ti.job_id,
 case when type = 'Upserting...' then
       case
        when _operation_type='_delta_staging' then 'MERGE_DELTA_STAGING_APPEND'
        when _operation_type='_base' then 'MERGE_BASE_UPSERT'
        else 'MERGE_OTHER' end
   when type in ('Purging...', 'Deleting', 'Purging', 'Cleaning') then 'CLEANUP'
   when type in ('Preparing', 'Compiling') then 'COMPILE'
   when type = 'Staging' then 'STAGE_UT'
   when type = 'Building' then 'BUILD_UDS'
   when type = 'Running' then
    case when IS_AUX=1 then 'AUX'
    else
      case
        when _operation_type='_delta' then 'ROUTE_DELTA'
        when _operation_type='_delta_staging_delta_index' then 'DELTA_STAGING_APPEND_INDEX'
        when _operation_type='_delta_staging_base_reindex' then 'DELTA_STAGING_REINDEX'
        when _operation_type='_base_delta_index' then 'DELTA_BASE_UPSERT_INDEX'
        when _operation_type='_delta_staging_base_reindex' then 'BASE_REINDEX'
        when _operation_type='_delta_delta_prep' then 'PREPARE_TABLE_PARAM'
        when _operation_type='_empty' then 'CREATE'
        else 'OTHER_DF' end
    end
   else '???' end as operation_type,
 d.ROW_COUNT as delta_row_count, ti.ROW_ID
from timer_plus ti left join <d> d
on d.UNIVERSE=ti.UNIVERSE and d.BATCH_ID=ti.BATCH_ID and ti.universe_table_name = d.TABLE_NAME
and d.BATCH_VERSION=ti.BATCH_VERSION
where type <> 'Compiling'"""  # d,t

def dispatcher(line):
    usage = argp.get_usage()
    input_params = argp.get_params(line)

    try:
        xshell_connector = XShell_Connector()
        client = xshell_connector.get_client()
        dispatcher = xshell_connector.get_uber_dispatcher()

        if 'refresh' in input_params:
            # root = '/var/log/xcalar/'
            # if 'root' in input_params:
            #     root = input_params['root']
            root = input_params['refresh']
            if root == '':
                root = '/var/log/xcalar/'
            print(f'-> Loading logs from {root}')
            dispatcher.update_or_create_datatarget(
                'LogMartDataTarget', 'sharednothingsymm', {'mountpoint': root})
            targetName = 'LogMartDataTarget'
            path = '/'
            fileNamePattern = ''
        elif 'load_asup' in input_params:
            targetName = 'Default Shared Root'
            path = input_params[
                'load_asup']    # /netstore/customers/customer4/3-27-20/asup/
            # fileNamePattern = 're:asup/node-.*/.*/supportData/syslog/.*$'
            fileNamePattern = 're:node-.*/.*/supportData/syslog/.*$'
        elif 'load_logs' in input_params:
            if 'target' in input_params:
                targetName = input_params['target']
            else:
                targetName = 'Default Shared Root'
            path = input_params[
                'load_logs']    # /netstore/customers/customer4/3-29-20/logs/
            fileNamePattern = ''
        else:
            print(usage)
            return

        # params = {'start_ts': '2020-03-28T21:00:00.000Z', 'end_ts': '9999-01-01T00:00:00.000Z'} # 'a_start_ts': '2020-03-28T21:00:00.000Z',
        params = {
            'start_ts': '2000-01-01T21:00:00.000Z',
            'end_ts': '9999-01-01T00:00:00.000Z'
        }
        for k, v in params.items():
            if k in input_params:
                params[k] = input_params[k]
            else:
                print(f'-> {k} not provided, setting to {v}')

        schema_controller = {
            'node_id': 'int',
            'source': 'string',
            'timestamp': 'timestamp',
            'process_id': 'int',
            'file_name': 'string',
            'log_level': 'string',
            'universe_id': 'string',
            'message': 'string',
            'messagelen': 'int',
            'lines': 'int'
        }
        s_table = load_source_table(
            dispatcher, client, 'logmart_controller_parser', schema_controller,
            targetName, path, fileNamePattern, 'CLR_MART_CONTROLLER')
        publish_table(dispatcher, s_table, 'CLR_LOG_RAW')

        # Build CONTROLLER
        t_table = execute_sql(
            dispatcher,
            SQL_PARSER, {
                **params, 't': f'`{s_table}`'
            },
            # pks=['UNIVERSE', 'RN'],
            prefix='clr_mart_parser')
        # publish_table(dispatcher, t_table, 'CLR_LOG_X')  #row_num_pk
        c_table = execute_sql(
            dispatcher,
            SQL_GET_BATCHID, {
                **params, 't': f'`{t_table}`'
            },
            pks=['UNIVERSE', 'BATCH_ID', 'BATCH_VERSION','RN'],
            prefix='clr_mart_get_batchid')
        publish_table(dispatcher, c_table, 'CLR_BATCHES')

        s_table = execute_sql(
            dispatcher,
            SQL_GET_STAGE, {
                **params, 't': f'`{t_table}`'
            },
            prefix='clr_mart_get_stage')
        controller = execute_sql(
            dispatcher,
            SQL_CONTROLLER,
            params={
                **params, 't': f'`{t_table}`',
                'c': f'`{c_table}`',
                's': f'`{s_table}`'
            },
            pks=['ROW_ID'],
            prefix='clr_mart_controller')
        publish_table(dispatcher, controller, 'CLR_LOG_PREP')
        # Build TABLES
        c_table = execute_sql(dispatcher, SQL_TABELS1, {
            **params, 't': f'`{controller}`'
        })
        tables_schema = {
            'UNIVERSE': 'string',
            'BATCH_ID': 'int',
            'BATCH_VERSION': 'int',
            'BATCH_UNIVERSE_ID': 'int',
            'TABLE_NAME': 'string',
            'ROW_COUNT': 'int'
        }    # , 'rn': 'int'}
        qb = QueryBuilder(
            schema=tables_schema)    # dispatcher.get_table(c_table).schema)
        qb.XcalarApiSynthesize(table_in=c_table, sameSession=False) \
          .XcalarApiMap({'T_INFO': 'explodeString(BATCH_INFO, ",")'}) \
          .XcalarApiMap({'TABLE_NAME': """cut(T_INFO,2,"'")""", 'ROW_COUNT': 'int(cut(T_INFO,2,": "))'}) \
          .XcalarApiFilter('and(not(contains(T_INFO,"$")),not(not(exists(BATCH_ID))))') \
          .XcalarApiIndex(keys={pk: tables_schema[pk] for pk in ['TABLE_NAME', 'UNIVERSE', 'BATCH_ID', 'BATCH_VERSION']})
        tables = dispatcher.execute(qb, prefix='clr_mart_tables').name
        publish_table(dispatcher, tables, 'CLR_INCREMENTS')

        # Build APPLICATIONS
        applications = execute_sql(
            dispatcher,
            SQL_APPLICTIONS,
            params={
                **params, 'c': f'`{controller}`',
                'x': f'`{tables}`'
            },
            pks=[
                'BATCH_UNIVERSE_ID', 'APPLICATION', 'SOURCE_UNIVERSE',
                'SOURCE_DELTA_TABLE'
            ],
            prefix='clr_mart_applications')
        publish_table(dispatcher, applications, 'CLR_APP_INPUTS')

        # Build STAGING
        stages = execute_sql(
            dispatcher,
            SQL_STAGES,
            params={
                **params, 't': f'`{controller}`',
                'd': f'`{tables}`'
            },
            pks=['ROW_ID'],
            prefix='clr_mart_stages')
        publish_table(dispatcher, stages, 'CLR_STAGES')

        dispatcher.unpinDropTables(prefix='ut_clr_mart_*')

        print('-> Sucessfully created published tabled in CONTROLLER MART')

    except KeyboardInterrupt:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
        return
    except Exception as err:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {err}')
        traceback.print_tb(err.__traceback__)


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


def execute_sql(dispatcher, sql_query, params, pks=None, prefix='clr_mart_'):
    for p, v in params.items():
        sql_query = sql_query.replace(f'<{p}>', f'{v}')

    tbl_name = dispatcher.session.execute_sql(
        sql_query=sql_query,
        result_table_name=dispatcher.nextTable(prefix='clr_mart_'))
    if pks:
        table = dispatcher.get_table(tbl_name)
        schema = table.schema
        qb = QueryBuilder(schema=schema)
        qb.XcalarApiSynthesize(table_in=tbl_name, columns=qb.synthesizeColumns(fromFP=True), sameSession=False) \
          .XcalarApiIndex(keys={pk: schema[pk] for pk in pks})
        tbl = dispatcher.execute(qb, desc='indexing', prefix=prefix)
        return tbl.name
    else:
        return tbl_name


def publish_table(dispatcher, session_table_name, pub_table_name):
    op = Operators(dispatcher.xcalarApi)
    try:
        op.unpublish(pub_table_name)
        print(f'Deleted previously published {pub_table_name} table')
    except Exception as e:
        if 'Publish table name not found' in str(e):
            pass
    op.publish(session_table_name, pub_table_name)
    print(f'-> Published {pub_table_name} table')


def load_ipython_extension(ipython, *args):
    argp.register_magic_function(ipython, dispatcher)


def unload_ipython_extension(ipython):
    pass
