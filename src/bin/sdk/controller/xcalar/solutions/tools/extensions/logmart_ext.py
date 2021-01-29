# logmart_ext.py

from colorama import Style
from xcalar.solutions.tools.connector import XShell_Connector
from xcalar.solutions.query_builder import QueryBuilder
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.solutions.tools.arg_parser import ArgParser
from pathlib import Path
import os
import traceback

SQL_INIT_FILTER = """with x as (
select source, min(rn) as start_rn, max(rn) as end_rn
from <t> t
where timestamp>cast('<start_ts>' as timestamp)
and timestamp<cast('<end_ts>' as timestamp)
group by 1)
select t.* from <t> t join x on t.source=x.source
and rn between start_rn and end_rn"""

SQL_CORE_LOG = """
with log as (select * from <l> where type='core_log'),
fuller as (select rn, source, ROW_NUMBER() OVER(PARTITION by source ORDER BY rn DESC) as log_rn
from log WHERE lines=1),
stack_ranges as (select
 f1.rn, f1.source, f2.rn-f1.rn as flag,
 coalesce(f2.rn,1000000000)-1 as rn_last,
 cast(f1.rn/<stack_size> as int) as range_tile
 from fuller f1 left join fuller f2
 on f1.source = f2.source and f2.log_rn=f1.log_rn+1),
stack_log as( select message, source, rn,
  cast(rn/<stack_size> as int) as range_tile
  from log where lines=0
  order by source, rn),
stacks as (select r.rn, r.source,
 collect_list (message) as stack
from stack_log l join stack_ranges r on l.source=r.source
 and nullif(r.flag,1) <> 0
and l.rn>r.rn and l.rn<=r.rn_last
group by 1,2),
prep as (select p.source, p.timestamp, s.stack, p.rn,
 sxdf_cut(p.MESSAGE,1,': ') as program,
 sxdf_cut(p.MESSAGE,2,': ') as module,
 sxdf_cut(p.MESSAGE,3,': ') as level,
 sxdf_cut(p.MESSAGE,7,': ') as txn,
 sxdf_cut(p.MESSAGE,2,concat(': Line',sxdf_cut(sxdf_cut(p.MESSAGE,2,': Line'),1,': '),': ')) as message,
 p.message as raw_msg
from  log p left join stacks s on p.source=s.source and p.rn=s.rn
where p.lines=1
ORDER BY p.source, p.rn)
select source, timestamp, program, module, level,
 case when txn like 'Txn %' then
     substring_index(substring(txn,5), ',', 1)
     else Null end as txn,
 case when txn like 'Txn %,%' then
     substring_index(txn, ',', -3)
     else Null end as txn2,
 message, raw_msg, stack, rn
from  prep p"""

SQL_SQLDF = """with prep as (
 select source, timestamp,  rn, program, raw_msg,
  case when module like '20%' then module else '' end as module,
  case when module like '20%' then substring(sxdf_cut(raw_msg,2, module), 3)
   else substring(sxdf_cut(raw_msg,2, program), 3) end as message
  from (select
   source, timestamp,  rn, message as raw_msg,
    sxdf_cut(MESSAGE,1,': ') as program,
    sxdf_cut(MESSAGE,2,': ') as module
  from <log>
where source like 'sqldf.out%')),
fuller as (select rn, source,
 ROW_NUMBER() OVER(PARTITION by source ORDER BY rn DESC) as log_rn
 from prep WHERE MODULE<>''),
stack_ranges as (select f1.rn, f1.source,
 f2.rn-f1.rn as flag, coalesce(f2.rn,1000000000)-1 as rn_last,
 cast(f1.rn/<stack_size> as int) as range_tile
 from fuller f1 left join fuller f2
 on f1.source = f2.source and f2.log_rn=f1.log_rn+1),
stack_log as(select message, source, rn,
 cast(rn/<stack_size> as int) as range_tile
 from prep where MODULE=''
 order by source, rn),
stacks as (select rn, source, collect_list(message) as stack
 from (select r.rn, r.source, message
  from stack_log l join stack_ranges r on l.source=r.source and l.range_tile=r.range_tile
  and nullif(r.flag,1) <> 0 and l.rn>r.rn and l.rn<=r.rn_last and l.rn<r.rn+<stack_size>
  union all
  select r.rn, r.source, message
  from stack_log l join stack_ranges r on l.source=r.source and l.range_tile=r.range_tile +1
  and nullif(r.flag,1) <> 0 and l.rn>r.rn and l.rn<=r.rn_last and l.rn<r.rn+<stack_size>
  order by source, rn)
 group by 1,2)
select p.source, p.timestamp, p.PROGRAM, p.message, s.stack, p.rn,
sxdf_cut(module,3,' ') as level, sxdf_cut(module,4,' ') as module
from  prep p left join stacks s on p.source=s.source and p.rn=s.rn
where p.MODULE<>''
ORDER BY p.source, p.rn"""

SQL_EXPSERVER_OUT = """with prep as (
 select source, timestamp,  rn, program, raw_msg,
  case when module like '[%' then substring(module,2) else '' end as module,
  case when module like '[%' then substring(sxdf_cut(raw_msg,2, module), 3)
   else substring(sxdf_cut(raw_msg,2, program), 3) end as message
  from (select
   source, timestamp,  rn, message as raw_msg,
    sxdf_cut(MESSAGE,1,': ') as program,
    sxdf_cut(MESSAGE,2,'] ') as module
  from <log>
where source like 'expserver.out%' )),
fuller as (select rn, source,
 ROW_NUMBER() OVER(PARTITION by source ORDER BY rn DESC) as log_rn
 from prep WHERE MODULE<>''),
stack_ranges as (select f1.rn, f1.source,
 f2.rn-f1.rn as flag, coalesce(f2.rn,1000000000)-1 as rn_last,
 cast(f1.rn/<stack_size> as int) as range_tile
 from fuller f1 left join fuller f2
 on f1.source = f2.source and f2.log_rn=f1.log_rn+1),
stack_log as(select message, source, rn,
 cast(rn/<stack_size> as int) as range_tile
 from prep where MODULE=''
 order by source, rn),
stacks as (select rn, source, collect_list(message) as stack
 from (select r.rn, r.source, message
  from stack_log l join stack_ranges r on l.source=r.source and l.range_tile=r.range_tile
  and nullif(r.flag,1) <> 0 and l.rn>r.rn and l.rn<=r.rn_last and l.rn<r.rn+<stack_size>
  union all
  select r.rn, r.source, message
  from stack_log l join stack_ranges r on l.source=r.source and l.range_tile=r.range_tile +1
  and nullif(r.flag,1) <> 0 and l.rn>r.rn and l.rn<=r.rn_last and l.rn<r.rn+<stack_size>
  order by source, rn)
 group by 1,2)
select p.source, p.timestamp, p.PROGRAM, ltrim(p.message) as message, s.stack, p.rn,
substring_index(module,' ',-1) as level, sxdf_cut(module,1,substring_index(module,' ',-1)) as module
from  prep p left join stacks s on p.source=s.source and p.rn=s.rn
where p.MODULE<>''
ORDER BY p.source, p.rn"""

SQL_XCM_CADDY_JUPYTER_OUT = """select source, timestamp, 'xcmonitor' as program, 'xcmonitor' as module,
 sxdf_cut(MESSAGE,2,': ') as message, message as raw_msg, rn,
 sxdf_cut(MESSAGE,1,': ') as level
from <log> where source like 'xcmonitor.out%'
union all
select source, timestamp, program, module,
  message, raw_msg, rn,
  case when module_level='I' then 'INFO'
   when module_level='W' then 'WARNING'
   when module_level='E' then 'ERROR'
   when message like '% | %' then sxdf_cut(message,1,' | ')
  else Null end as level
 from(
  select
   source, timestamp, program, module, RN,
   case when module='' then sxdf_cut(MESSAGE,2,': ')
    else sxdf_cut(MESSAGE,2,'] ') end as message,
   sxdf_cut(module,1,' ') as module_level,
   message as raw_msg
   from (
    select source, timestamp, message, rn,
    sxdf_cut(MESSAGE,1,': ') as program,
    sxdf_cut(sxdf_cut(MESSAGE,1,'] '),2,': [') as module
    from <log>
 where source like 'jupyter.out%'))
union all
 select source, timestamp, program, module,
  case when isnull(level) then
    substring_index(MESSAGE, concat(level,'] '),-1)
  else MESSAGE end as message, raw_msg, rn, level
 from (select
  source, timestamp,
   sxdf_cut(sxdf_cut(MESSAGE,1,': '),1,' ') as program,
   sxdf_cut(sxdf_cut(MESSAGE,1,': '),2,' ') as module,
   case when MESSAGE like '%[ERROR%' then 'ERROR'
     when MESSAGE like '%[INFO%' then 'INFO'
     else Null end as level,
   case when MESSAGE like '%[ERROR%' or MESSAGE like '%[INFO%' then
       substring(sxdf_cut(MESSAGE,2,substring_index(MESSAGE,' ',5)),2)
     else substring(sxdf_cut(MESSAGE,2,substring_index(MESSAGE,' ',4)),2)
     end as message,
   message as raw_msg, rn
 from <log> where source like 'caddy.out%')"""

SQL_CADDY_LOG = """select source, timestamp,
 sxdf_cut(MESSAGE,2,'"') as module,
 IP as program,
 sxdf_cut(MESSAGE,4,' ') as level,
 ltrim(sxdf_cut(MESSAGE,2,substring_index(MESSAGE,' ',4))) as message,
 message as raw_msg, rn
from <log> where TYPE='caddy_log'"""

# Cgroup App log format is not standard, remove for now
SQL_XPU_OUT = """with log as (select * from <l> where type='xpu_out' and message not like '%Cgroup App%')
, fuller as (select rn, source,
 ROW_NUMBER() OVER(PARTITION by source ORDER BY rn DESC) as log_rn
 from log WHERE lines=1)
,stack_ranges as
(select  f1.rn, f1.source, f2.rn-f1.rn as flag,
 coalesce(f2.rn,1000000000)-1 as rn_last,
 cast(f1.rn/<stack_size> as int) as range_tile
 from fuller f1 left join fuller f2
 on f1.source = f2.source and f2.log_rn=f1.log_rn+1),
stack_log as( select message, source, rn,
    cast(rn/<stack_size> as int) as range_tile
    from log where lines=0 order by source, rn),
stacks as (select rn, source, collect_list(message) as stack
 from (
  select r.rn, r.source, message
  from stack_log l join stack_ranges r on l.source=r.source and l.range_tile=r.range_tile
  and nullif(r.flag,1) <> 0 and l.rn>r.rn and l.rn<=r.rn_last and l.rn<r.rn+<stack_size>
  union all
  select r.rn, r.source, message
  from stack_log l join stack_ranges r on l.source=r.source and l.range_tile=r.range_tile +1
  and nullif(r.flag,1) <> 0 and l.rn>r.rn and l.rn<=r.rn_last and l.rn<r.rn+<stack_size>
  order by source, rn)
 group by 1,2)
select p.source, p.timestamp, s.stack, 'XPU' as program,
 sxdf_cut(p.MESSAGE,2,' - ') as module,
 sxdf_cut(p.MESSAGE,3,' - ') as level,
 sxdf_cut(sxdf_cut(p.MESSAGE,3,' - '),2,'Txn') as txn,
 substring(sxdf_cut(p.MESSAGE,2,substring_index(p.MESSAGE,' - ',3)),4) as message,
 p.message as raw_msg, p.rn
from  log p left join stacks s on p.source=s.source and p.rn=s.rn
where p.lines=1
"""

argp = ArgParser('logmart', [
    {
        'verbs': {
            'refresh': 'log_root'
        },
        'desc': 'Refresh LogMart tables from the cluster logs.'
    },
    {
        'verbs': {
            'load_asup': 'asup_path',
            'target': 'target_name'
        },
        'desc': 'Load LogMart tables from the specified ASUP location.'
    },
    {
        'verbs': {
            'load_logs': 'logs_path',
            'target': 'target_name'
        },
        'desc': 'Load LogMart tables from the specified untarred location.'
    },
])


def dispatcher(line):
    usage = argp.get_usage()
    input_params = argp.get_params(line)

    try:
        xshell_connector = XShell_Connector()
        client = xshell_connector.get_client()
        # xcalarApi = xshell_connector.get_xcalarapi()
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
            if 'target' in input_params:
                targetName = input_params['target']
            else:
                targetName = 'Default Shared Root'
            path = input_params[
                'load_asup']    # /netstore/customers/customer4/3-27-20/asup/
            fileNamePattern = 're:asup/node-.*/.*/supportData/syslog/.*$'
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
            'stack_size': 20,
            'start_ts': '2000-01-01T21:00:00.000Z',
            'end_ts': '9999-01-01T00:00:00.000Z'
        }
        is_filtered = 'start_ts' in input_params or 'end_ts' in input_params
        for k, v in params.items():
            if k in input_params:
                params[k] = input_params[k]
            else:
                print(f'-> {k} not provided, setting to {v}')

        schema_all = {
            'node_id': 'int',
            'source': 'string',
            'type': 'string',
            'timestamp': 'timestamp',
            'ip': 'string',
            'message': 'string',
            'lines': 'int',
            'rn': 'int'
        }

        s_table = load_source_table(dispatcher, client, 'logmart_all_parser',
                                    schema_all, targetName, path,
                                    fileNamePattern, 'LOGMART')
        if is_filtered:
            s_table = execute_sql(
                dispatcher,
                SQL_INIT_FILTER, {
                    **params, 't': f'`{s_table}`'
                },
                pks=['RN', 'SOURCE'],
                prefix='logmart_all')

        publish_table(dispatcher, s_table, 'LOGMART')

        core_table = execute_sql(
            dispatcher,
            SQL_CORE_LOG, {
                **params, 'l': f'`{s_table}`'
            },
            pks=['RN', 'SOURCE'],
            prefix='logmart_core')
        publish_table(dispatcher, core_table, 'LOGMART_CORE')

        build_table(
            dispatcher,
            SQL_SQLDF,
            pks=['RN', 'SOURCE'],
            params={
                **params, 'log': f'`{s_table}`'
            },
            prefix='sqldf_out')

        build_table(
            dispatcher,
            SQL_EXPSERVER_OUT,
            pks=['RN', 'SOURCE'],
            params={
                **params, 'log': f'`{s_table}`'
            },
            prefix='exps_out')

        build_table(
            dispatcher,
            SQL_XCM_CADDY_JUPYTER_OUT,
            pks=['RN', 'SOURCE'],
            params={
                **params, 'log': f'`{s_table}`'
            },
            prefix='xc_out')

        build_table(
            dispatcher,
            SQL_CADDY_LOG,
            pks=['RN', 'SOURCE'],
            params={
                **params, 'log': f'`{s_table}`'
            },
            prefix='caddy')

        build_table(
            dispatcher,
            SQL_XPU_OUT,
            pks=['RN', 'SOURCE'],
            params={
                **params, 'l': f'`{s_table}`'
            },
            prefix='xpu_out')

        dispatcher.unpinDropTables(prefix='ut_logmart_*')

        print('-> Sucessfully created published tabled in LOG MART')

    except KeyboardInterrupt:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
        return
    except Exception as err:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {err}')
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
