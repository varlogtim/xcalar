from xcalar.external.dataflow import Dataflow
from xcalar.external.runtime import Runtime
from xcalar.external.Retina import Retina
from xcalar.solutions.query_builder import QueryBuilder
from xcalar.solutions.timer import TimerMsg
from xcalar.solutions.xcalar_client_utils import clone_client, get_or_create_session, drop_tables
from xcalar.external.table import Table
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException
from xcalar.external.exceptions import XDPException

import os
from pathlib import Path
import json
import re
import random
from datetime import datetime
from structlog import get_logger
import ast
import time

logger = get_logger(__name__)
DTMAP = {
    'string': 'blah',
    'integer': 1,
    'int': 1,
    'float': '0.1',
    'timestamp': datetime.now().timestamp() * 1000
}


class UberDispatcher:
    def __init__(self,
                 xcalarClient,
                 session,
                 xcalarApi,
                 dsetSeed='uds_kafka',
                 fatPtr='fatPtr',
                 parserName='gs_kafka:parse_kafka_topic',
                 targetName='kafka-base',
                 tgroup='ut',
                 isDebug=False,
                 xcalarProperties=None,
                 scheduler=Runtime.Scheduler.Sched1,
                 shared_session=False,
                 parallel_operations=False):

        self.xcalarClient = xcalarClient
        self.workbook = xcalarClient.get_workbook(session.name)
        self.session = session
        self.xcalarApi = xcalarApi
        self.dsetSeed = dsetSeed
        self.fatPtr = fatPtr
        self.parserName = parserName
        self.targetName = targetName
        self.isDebug = isDebug
        self.tgroup = tgroup
        self.datasetCounter = 0
        self.xcalarProperties = xcalarProperties
        self.scheduler = scheduler
        self.check_udf('dml_insert', '/udf/dml_insert.py')
        self.parallel_operations = parallel_operations
        self.shared_session = shared_session
        self.pause_retry_counts = 3600
        self.pause_retry_seconds = 5    # wait up to 5 hours if this is a consuming session

    def get_table(self, table):
        if type(table) == str:
            xcalarClient = clone_client(self.xcalarClient)
            session = get_or_create_session(xcalarClient, self.session.name)
            return Table(session, table)
        else:
            return table

    # Makes a session-local table with the given name globally visible and readable.
    # Returns the fully qualified, globally visible name of the table.
    def publish_table(self, name):
        if not self.shared_session:
            return name
        table = self.get_table(name)
        assert (table)
        for i_retry in range(self.pause_retry_counts + 1):
            try:
                fq_name = table.publish()
                break
            # workaround: these errors should be handled inside the API
            except XcalarApiStatusException as e:
                if e.status == StatusT.StatusAccess and i_retry < self.pause_retry_counts:
                    logger.debug(f'***: Permission denied publishing {name}')
                    time.sleep(self.pause_retry_seconds)
                else:
                    raise (e)
            except XDPException as e:
                if e.statusCode == StatusT.StatusAccess and i_retry < self.pause_retry_counts:
                    logger.debug(f'***: Permission denied publishing {name}')
                    time.sleep(self.pause_retry_seconds)
                else:
                    raise (e)
        assert (fq_name)
        return fq_name

    def nextTable(self, prefix='', tgroup='ut'):
        tm = str(datetime.now().timestamp()).replace(
            '.', '#')    # replaced # with _, replace back for SQL to work
        return '{}_{}{}'.format(tgroup, prefix, tm)

    def nextDatasetName(self, visibleName=None):
        if visibleName:
            return '{}.{}.{}'.format(self.xcalarClient.username,
                                     random.randint(10000, 99999), visibleName)
        else:
            return '{}.{}.{}'.format(self.xcalarClient.username,
                                     datetime.now().timestamp(), self.dsetSeed)

    # workaround UDF parameter limitation (1278 chars) - store params in kvStore and pass kvStore key to UDF
    def send_2kvstore(self, value, key_prefix=''):
        kvstore = self.xcalarClient.global_kvstore()
        key = f'controller_{self.nextTable(key_prefix)}_{self.session.session_id}'
        kvstore.add_or_replace(key, value, persist=False)
        return key

    # workaround UDF parameter limitation (1278 chars) - clear old keys
    def clean_kvstore(self, key_prefix=''):
        kvstore = self.xcalarClient.global_kvstore()
        # for k in kvstore.list(f'controller_{self.tgroup}_*_{self.session.session_id}'):
        for k in kvstore.list(f'controller_{self.tgroup}_*'):
            if k.split('_')[-1] == self.session.session_id:
                logger.info(f'deleting... {k}')
                try:
                    kvstore.delete(k)
                except Exception as e:
                    # todo: need to understand why this method sometimes fails
                    logger.debug(f'***: {str(e)}')

    def purge_queries(self):
        try:
            for q in self.xcalarApi.listQueries(
                    f'*_{self.session.name}-{self.xcalarClient.username}-*'
            ).queries:
                if q.state == 'qrError':
                    logger.info(f'deleting... {q.name}')
                    try:
                        self.xcalarApi.deleteQuery(q.name)
                    except Exception as e:
                        logger.debug(f'***: {str(e)}')
        except Exception as e:
            logger.debug(f'***: {str(e)}')

    # update shared or workbook UDF
    def check_udf(self, udf_name, path, local_scope=True, check=True):
        container = self.workbook if local_scope else self.xcalarClient
        udf = None
        try:
            udf = container.get_udf_module(udf_name)
            if not check:
                return True
        except Exception as e:
            logger.warning(f'***: {e} checking {udf_name}')
            pass
        udf_file = (os.path.dirname(Path(__file__)) + path)
        with open(udf_file) as f_udf:
            udf_source = f_udf.read()
        if udf:
            if udf._get() == udf_source:
                logger.info(
                    f'reusing existing {"workbook" if local_scope else "shared"} udf: {udf_name}'
                )
                return True
            else:
                udf.delete()
        logger.warning(f'updating: {udf_name}')
        for try_counts in range(1, 6):
            try:
                container.create_udf_module(udf_name, udf_source)
                return True
            except Exception as e:
                logger.debug(
                    '***: WARNING updating shared UDF: {}, ATTEMPT# {}'.format(
                        str(e), try_counts))
        raise Exception(
            '***: ERROR 5 unsuccessful attempts to update shared UDF, exiting')

    # get UDFs used by a dataflow in order to upload them to source-control
    def extract_udf_from_dataflow(self, dataflow, check_if_exists=True):
        evalStrings = set()
        qs = self.get_query_strings(dataflow)['query']
        for o in qs:
            for e in o.get('args', []).get('eval', []):
                evalStrings.add(e['evalString'])
        ee = '|'.join(evalStrings)
        mapUdfs = re.findall(r'([_a-zA-Z0-9]+):[_a-zA-Z0-9]+\(', ee)
        parseUdfs = [
            e.get('args', {}).get('loadArgs', {}).get('parseArgs', {}).get(
                'parserFnName', ':').split(':')[0] for e in qs
        ]
        return set(mapUdfs + parseUdfs)

    def update_or_create_shared_udf(self,
                                    udf_name,
                                    udf_source,
                                    unique_name=True):
        if unique_name:
            for u in self.xcalarClient.list_udf_modules(f'{udf_name}_*'):
                u.delete()
            tm_name = '{}_{}'.format(
                udf_name,
                str(datetime.now().timestamp()).replace('.', '_'))
        else:
            try:
                udf = self.xcalarClient.get_udf_module(udf_name)
                if udf._get() == udf_source:
                    logger.info(f'reusing existing shared udf: {udf_name}')
                    return udf_name
                else:
                    udf.delete()
            except Exception:
                pass
            tm_name = udf_name
        # this workaround should be removed after Devang's async-udf bug update fix is tested
        for try_counts in range(1, 6):
            try:
                self.xcalarClient.create_udf_module(tm_name, udf_source)
                return tm_name
            except Exception as e:
                logger.debug(
                    '***: WARNING updating shared UDF: {}, ATTEMPT# {}'.format(
                        str(e), try_counts))
        raise Exception(
            '***: ERROR 5 unsuccessful attempts to update shared UDF, exiting')

    def update_or_create_udf(self, udf_name, udf_source, unique_name=True):
        if unique_name:
            for u in self.workbook.list_udf_modules(f'{udf_name}_*'):
                u.delete()
            tm_name = '{}_{}'.format(
                udf_name,
                str(datetime.now().timestamp()).replace('.', '_'))
        else:
            try:
                udf = self.workbook.get_udf_module(udf_name)
                if udf._get() == udf_source:
                    logger.info(f'reusing existing udf: {udf_name}')
                    return udf_name
                else:
                    udf.delete()
            except Exception:
                pass
            tm_name = udf_name
        # this workaround should be removed after Devang's async-udf bug update fix is tested
        for try_counts in range(1, 6):
            try:
                self.workbook.create_udf_module(tm_name, udf_source)
                return tm_name
            except Exception as e:
                logger.debug(
                    '***: WARNING updating UDF: {}, ATTEMPT# {}'.format(
                        str(e), try_counts))
        raise Exception(
            '***: ERROR 5 unsuccessful attempts to update UDF, exiting')

    def update_or_create_datatarget(self,
                                    target_name,
                                    target_type_id,
                                    params={}):
        try:
            if target_name in [
                    x.name for x in self.xcalarClient.list_data_targets()
            ]:
                data_target = self.xcalarClient.get_data_target(target_name)
                if data_target.type_id == target_type_id and min([params[d] == data_target.params.get(d, None) for d in params] + [True]) and \
                        min([data_target.params[d] == params.get(d, None) for d in data_target.params] + [True]):
                    return
                data_target.delete()
            self.xcalarClient.add_data_target(
                target_name=target_name,
                target_type_id=target_type_id,
                params=params)
        except Exception as e:
            raise Exception('***: Error updating "{}" data target: {}'.format(
                target_name, str(e)))

    def get_query_strings(self, dataflow):
        if type(dataflow) == str:
            text = dataflow
        else:
            text = dataflow._optimized_query_string
        optimized = json.loads(text)
        retina = json.loads(optimized['retina'])
        query = json.loads(retina['query'])
        return {'optimized': optimized, 'retina': retina, 'query': query}

    def loadStreamNonOptimized(self,
                               data_target,
                               parser_name,
                               parser_args={},
                               pin_results=False):
        dsetName = self.nextDatasetName(visibleName=self.dsetSeed)
        table_name = self.nextTable('uber')
        with TimerMsg(f'Building Uber Dataset') as tm:
            db = self.workbook.build_dataset(
                dataset_name=dsetName,
                data_target=data_target,
                path='1',
                parser_args=parser_args,
                import_format='udf',
                parser_name=parser_name)
            # do load as dataflow (as_df=True) so stats are obtained for load
            db.load(as_df=True)
            logger.debug(tm.get())
        with TimerMsg(f'Staging Uber Table {table_name}') as tm:
            self.datasetCounter += 1
            qb = QueryBuilder(schema={}) \
                .XcalarApiIndex(keys={'xcalarRecordNum': 'DfUnknown'},
                                table_in='.XcalarDS.{}'.format(dsetName),
                                prefix=self.fatPtr)

            qs = json.dumps(qb.getQuery())
            df = Dataflow.create_dataflow_from_query_string(
                self.xcalarClient, query_string=qs)
            df_name = self.session.execute_dataflow(
                df,
                table_name=table_name,
                optimized=False,
                is_async=False,
                pin_results=True,
                sched_name=self.scheduler)
            out_table = Table(self.session, table_name)
            self.dropDataflow(self.session, df_name)
            logger.debug(tm.get(f'records: {out_table.record_count()}'))
        return (out_table, dsetName)

    def unpinTable(self, table):
        try:
            table.unpin()
        except Exception:
            pass

    def dropTable(self, table):
        try:
            self.unpinTable(table)
            table.drop(delete_completely=True)
        except Exception as e:
            logger.debug(f'***: dropping {table.name} - {str(e)}')

    def unpinDropTables(self, tnames_to_keep=[], prefix=None):
        if prefix is None:
            prefix = f'{self.tgroup}_*'
        for t in self.session.list_tables(prefix):
            if t.name not in tnames_to_keep:
                try:
                    t.unpin()
                    logger.info(f'unpinning ... {t.name} - succeed')
                except Exception as e:
                    logger.info(f'unpinning ... {t.name} - failed, {str(e)}')
        drop_tables(self.session, prefix)

    def query_name(self, table_name):
        return f"{table_name.replace('#', '_')}_{self.session.name}"

    # note - this is to address XCE backwaord incompatibility
    # Xcalar version before 2.0.5 RC21 will not work
    def df_query_name(self, table_name):
        return ''

    def executeRetry(self,
                     session,
                     df,
                     params,
                     query_name,
                     table_name=None,
                     desc='',
                     clean_job_state=True):
        with TimerMsg(f'Running {desc}, {table_name}') as tm:
            # df_name = self.executeRetry(session, df, params=params, table_name=table_name,
            #                             query_name=self.df_query_name(table_name))
            for i_retry in range(self.pause_retry_counts + 1):
                try:
                    df_name = session.execute_dataflow(
                        df,
                        table_name=table_name,
                        params=params,
                        optimized=True,
                        is_async=False,
                        query_name=query_name,
                        pin_results=True,
                        sched_name=random.choice(Runtime.get_dataflow_scheds()),
                        parallel_operations=self.parallel_operations,
                        clean_job_state=clean_job_state)
                    break
                # workaround: these errors should be handled inside the API
                except XcalarApiStatusException as e:
                    if e.status == StatusT.StatusAccess and i_retry < self.pause_retry_counts:
                        logger.debug(
                            f'***: Permission denied executing {query_name}')
                        time.sleep(self.pause_retry_seconds)
                    else:
                        raise (e)
                except XDPException as e:
                    if e.statusCode == StatusT.StatusAccess and i_retry < self.pause_retry_counts:
                        logger.debug(
                            f'***: Permission denied executing {query_name}')
                        time.sleep(self.pause_retry_seconds)
                    else:
                        raise (e)
            out_table = Table(session, table_name)
            if table_name:
                logger.debug(tm.get(f'dataflow {df_name}; records: {out_table.record_count()}'))
            else:
                logger.debug(tm.get(f'dataflow {df_name};'))
            if clean_job_state:
                self.dropDataflow(session, df_name)
        return out_table

    def execute(self,
                query,
                schema=None,
                params=None,
                desc='',
                prefix='',
                tgroup=None,
                forced_name=None,
                pin_results=False):
        xcalarClient = clone_client(self.xcalarClient)
        session = get_or_create_session(xcalarClient, self.session.name)
        with TimerMsg(f'Compiling query {prefix}, {desc}') as tm:
            if tgroup is None:
                tgroup = self.tgroup
            table_name = forced_name if forced_name else self.nextTable(
                prefix, tgroup)
            if type(query) != str:
                schema = query.schema
                query = json.dumps(query.getQuery())
            if params:
                for p in params:
                    if type(params[p]) != str:
                        raise Exception(
                            'Value of parameter {} must be string, not {}'.
                            format(p, type(params[p])))
            columnsOut = [{'columnName': c, 'headerAlias': c} for c in schema]
            df = Dataflow.create_dataflow_from_query_string(
                xcalarClient,
                query_string=query,
                columns_to_export=columnsOut,
                dataflow_name=self.query_name(table_name))
            logger.debug(tm.get())
        for try_counts in range(1, 6):
            try:
                if 'XcalarApiExport' in query:
                    table_name = None
                else:
                    table_name = forced_name if forced_name else self.nextTable(
                        prefix, tgroup)
                return self.executeRetry(
                    session,
                    df,
                    params=params,
                    table_name=table_name,
                    query_name=self.df_query_name(table_name),
                    desc=desc)
            except Exception as e:
                # XCE/middle-tier workaround: prev execute errored out while actually it did create a table
                if forced_name and type(
                        e
                ) == XcalarApiStatusException and e.status == StatusT.StatusTableAlreadyExists:
                    logger.debug(
                        f'***: Table {table_name} already exists. Skipping execution'
                    )
                    return Table(session, table_name)
                # this workaround should be removed after Devang's async-udf bug update fix is tested
                logger.debug(
                    f'***: WARNING executing {desc}: {str(e)}, ATTEMPT# {try_counts}'
                )
                if try_counts == 5:
                    logger.debug(query)
                    raise Exception(
                        f'***: 5 unsuccessful attempts loading {table_name} failed, exiting'
                    )

    def dropRetinas(self):
        try:
            ret = Retina(self.xcalarApi)
            dfs = ret.list().retinaDescs
            for df in dfs:
                dfparts = df.retinaName.split('-')
                if len(dfparts) > 3 and dfparts[1] == self.session.session_id:
                    ret.delete(df.retinaName)
                    logger.info(f'Dropping retina... {df.retinaName}')
        except Exception:
            pass

    def dropDataflow(self, session, df_name):
        try:    # this method will work in RC23, but will fail in earlier versions
            session.delete_job_state(df_name)
            return True
        except Exception as x:
            return False

    def getParams(self, wbk_name=None):
        wbk_name = wbk_name if wbk_name else self.workbook.name
        kvsw = self.xcalarClient.global_kvstore() \
            .workbook_by_name(self.xcalarClient,
                              self.xcalarClient._user_name, wbk_name)
        params = {}
        try:
            params = json.loads(kvsw.lookup('gDagParamKey-1'))
        except Exception:
            logger.error(
                '***: Warning Failed getting parameters from the workbook, assuming none defined'
            )
        return params

    def getDataflow(self, df_name, wbk_name=None, post_processor=None):
        workbook = self.xcalarClient.get_workbook(
            wbk_name) if wbk_name else self.workbook
        df = workbook.get_dataflow(df_name)
        dfs = self.get_query_strings(df)
        # qry = self.__preprocess(dfs['query'])
        qry = dfs['query']
        if post_processor:
            qry = post_processor(qry)
        qs = json.dumps(qry)
        dfs['retina']['query'] = qs
        dfs['optimized']['retina'] = json.dumps(dfs['retina'])
        optimized_query = json.dumps(dfs['optimized'])
        return (qs, optimized_query)

    def executeDataflow(self,
                        dataflow,
                        params=None,
                        desc='',
                        other_workbook=None,
                        prefix='',
                        tgroup=None,
                        forced_name=None,
                        post_processor=None,
                        pin_results=False):
        xcalarClient = clone_client(self.xcalarClient)
        session = get_or_create_session(xcalarClient, self.session.name)
        with TimerMsg(f'Preparing query {prefix}, {desc}') as tm:
            if tgroup is None:
                tgroup = self.tgroup
            table_name = forced_name if forced_name else self.nextTable(
                prefix, tgroup)
            if type(dataflow) == str:
                dfs = self.getDataflow(
                    df_name=dataflow,
                    wbk_name=other_workbook,
                    post_processor=post_processor)
                dataflow = Dataflow.create_dataflow_from_query_string(
                    self.xcalarClient,
                    query_string=dfs[0],
                    optimized_query_string=dfs[1],
                    dataflow_name=self.query_name(table_name))
            else:
                dataflow.name = self.query_name(table_name)
            logger.debug(tm.get())

        for try_counts in range(1, 6):
            try:
                table_name = forced_name if forced_name else self.nextTable(
                    prefix, tgroup
                )    # XCE Bug workaround: execute fails even though it does create a table
                out_table = self.executeRetry(
                    session,
                    dataflow,
                    params=params,
                    table_name=table_name,
                    query_name=self.df_query_name(table_name),
                    clean_job_state=False)
                break
            except Exception as e:
                logger.debug(
                    f'***: WARNING executing {desc}: {str(e)}, ATTEMPT# {try_counts}'
                )
                if try_counts == 5:
                    # logger.debug(query)
                    raise Exception(
                        f'***: 5 unsuccessful attempts loading {table_name} failed, exiting'
                    )
        return out_table

    def executeMergeRetry(self, base_table, delta_table, desc):
        for i_retry in range(self.pause_retry_counts + 1):
            try:
                with TimerMsg(f'{desc}... {base_table.name}') as tm:
                    logger.debug(f'Merging {desc} delta table: {delta_table.name}: {delta_table.record_count()} rows into {base_table.name}: {base_table.record_count()} rows')
                    base_table.merge(delta_table)
                    logger.debug(tm.get(f'records: {base_table.record_count()}'))
                break
            # XCE workaround: these errors should be handled inside the API
            except XcalarApiStatusException as e:
                if e.status == StatusT.StatusAccess and i_retry < self.pause_retry_counts:
                    logger.debug(
                        f'***: Permission denied {desc} {base_table.name}')
                    time.sleep(self.pause_retry_seconds)
                else:
                    raise (e)
            except XDPException as e:
                if e.statusCode == StatusT.StatusAccess and i_retry < self.pause_retry_counts:
                    logger.debug(
                        f'***: Permission denied {desc} {base_table.name}')
                    time.sleep(self.pause_retry_seconds)
                else:
                    raise (e)
            except Exception as e:
                raise (e)

    def executeMerge(self,
                     t_name,
                     base_table,
                     qb_delta,
                     sortkeys,
                     desc='Merging'):
        prep_d = self.execute(qb_delta, prefix=f'{t_name}_delta_index')
        if prep_d.record_count() == 0:
            logger.debug(f'Not {desc} {t_name}: no records to modify')
            return base_table
        try:
            self.executeMergeRetry(base_table, prep_d, desc)
            return base_table
        # if base table keys were not the same as deltas or missing - reindex base table
        except Exception as e:
            key_attr = base_table.get_meta().keys
            logger.debug(
                f'***: Warning {e}, {t_name} current keys: {key_attr}, new keys:{sortkeys} reindexing'
            )
            logger.debug(f'Schema {t_name} base: {base_table.schema}')
            logger.debug(f'Keys {t_name} delta: {prep_d.get_meta().keys}')
            logger.debug(f'Schema {t_name} delta: {prep_d.schema}')

            qb = QueryBuilder(schema=base_table.schema)
            qb.XcalarApiSynthesize(table_in=base_table.name, columns=qb.synthesizeColumns(), sameSession=False) \
                .XcalarApiSort(sortkeys) \
                .XcalarApiSynthesize(columns=qb.synthesizeColumns())

            new_base = self.execute(
                qb, prefix=f'{t_name}_base_reindex', pin_results=True)
            self.executeMergeRetry(new_base, prep_d, desc)
            logger.debug(
                f'{t_name} Keys after merge: {base_table.get_meta().keys}')
            return new_base

    def executeUpsert(self,
                      t_name,
                      base,
                      delta_name,
                      pklist,
                      milestone_id,
                      prefix=''):
        base_table = self.get_table(base)
        sortkeys = {k: 'PartialAscending' for k in pklist}
        qb = QueryBuilder(schema={
            **base_table.schema, 'XcalarOpCode': 'int',
            'XcalarRankOver': 'int'
        })
        qb.XcalarApiSynthesize(table_in=delta_name, sameSession=False, columns=qb.synthesizeColumns()) \
            .XcalarApiSort(sortkeys) \
            .XcalarApiMap({'XcalarOpCode': 'absInt(1)', 'XcalarRankOver': f'int({milestone_id})'}) \
            .XcalarApiSynthesize(columns=qb.synthesizeColumns())
        return self.executeMerge(
            t_name, base_table, qb, sortkeys, desc='Upserting')

    def executePurge(self,
                     t_name,
                     base,
                     filter_eval,
                     pklist,
                     milestone_id=1,
                     prefix=''):
        base_table = self.get_table(base)
        sortkeys = {k: 'PartialAscending' for k in pklist}
        qb = QueryBuilder(schema={
            **base_table.schema, 'XcalarOpCode': 'int',
            'XcalarRankOver': 'int'
        })
        qb.XcalarApiSynthesize(table_in=base_table.name, sameSession=False, columns=qb.synthesizeColumns()) \
            .XcalarApiFilter(filter_eval) \
            .XcalarApiSort(sortkeys) \
            .XcalarApiMap({'XcalarOpCode': 'absInt(0)', 'XcalarRankOver': f'int({milestone_id})'}) \
            .XcalarApiSynthesize(columns=qb.synthesizeColumns())
        return self.executeMerge(
            t_name, base_table, qb, sortkeys, desc='Purging')

    def rename(self, src_name, target_name):
        src = self.get_table(src_name)
        qb = QueryBuilder(schema=src.schema)
        qb.XcalarApiSynthesize(
            table_in=src_name,
            columns=qb.synthesizeColumns(),
            sameSession=False)
        out_table = self.execute(
            qb, schema=src.schema, forced_name=target_name)
        return out_table

    def clone(self, src_name, prefix='_'):
        src = self.get_table(src_name)
        qb = QueryBuilder(schema=src.schema)
        qb.XcalarApiSynthesize(
            table_in=src_name,
            columns=qb.synthesizeColumns(),
            sameSession=False)
        return self.execute(qb, desc='clonning', prefix=prefix)

    def loadFromCSV(
            self,
            schema,
            targetName,
            path,
            isFolder=False,
            parseArgs={
                'recordDelim': '\n',
                'fieldDelim': '\t',
                'isCRLF': False,
                'linesToSkip': 1,
                'quoteDelim': '"',
                'hasHeader': True,
                'schemaFile': '',
                'schemaMode': 'loadInput'
            },
            prefix='',
            tgroup='ut',
            pk=None):
        qb = QueryBuilder(schema)
        qb.XcalarApiBulkLoad(parserArg=parseArgs,
                             targetName=targetName,
                             parserFnName='default:parseCsv',
                             path=path,
                             pk=pk,
                             recursive=isFolder) \
            .XcalarApiSynthesize(columns=qb.synthesizeColumns())
        return self.execute(qb, schema=qb.schema, prefix=prefix)

    def loadFromSnapshotCSV(
            self,
            schema,
            targetName,
            path,
            isFolder=False,
            parseArgs={
                'recordDelim': '\n',
                'fieldDelim': '\t',
                'isCRLF': False,
                'linesToSkip': 1,
                'quoteDelim': '"',
                'hasHeader': True,
                'schemaFile': '',
                'schemaMode': 'loadInput',
                'dialect': 'xcalarSnapshot'
            },
            prefix='',
            tgroup='ut',
            pk=None,
            forced_name=None):
        qb = QueryBuilder(schema)
        qb.XcalarApiBulkLoad(parserArg=parseArgs,
                             targetName=targetName,
                             parserFnName='default:parseCsv',
                             path=path,
                             fileNamePattern='*.csv.gz',
                             pk=pk,
                             recursive=isFolder) \
            .XcalarApiSynthesize(columns=qb.synthesizeColumns())
        return self.execute(
            qb, schema=qb.schema, prefix=prefix, forced_name=forced_name)

    def loadFromParquet(self,
                        schema,
                        targetName,
                        path,
                        isFolder=False,
                        parseArgs={'parquetParser': 'native'},
                        prefix='',
                        tgroup='ut',
                        forced_name=None):
        qb = QueryBuilder(schema)
        qb.XcalarApiBulkLoad(parserArg=parseArgs,
                             targetName=targetName,
                             parserFnName='/sharedUDFs/default:parseParquet',
                             fileNamePattern='re:.*parquet$',
                             path=path,
                             recursive=isFolder) \
            .XcalarApiSynthesize(columns=qb.synthesizeColumns())
        return self.execute(
            qb,
            schema=schema,
            prefix=prefix,
            tgroup='ut',
            forced_name=forced_name)

    def replace(self, base_table, where=[], repalce_with=None):
        """
        Delete table records
        or replace records with new set
        """
        schema = self.get_table(base_table).schema
        evalo = None
        for kdict in where:
            evala = None
            for k in kdict:
                if schema[k] == 'DfString':
                    ev = "eq({},'{}')".format(k, kdict[k])
                else:
                    ev = 'eq({},{})'.format(k, kdict[k])
                evala = 'and({},{})'.format(evala, ev) if evala else ev
            evalo = 'or({},{})'.format(evalo, evala) if evalo else evala
        print(evalo)
        qb = QueryBuilder(schema)
        qb.XcalarApiSynthesize(table_in=base_table, sameSession=False) \
            .XcalarApiFilter('not({})'.format(evalo))
        if repalce_with and len(repalce_with) > 0:
            return self.insertFromJson(
                records=repalce_with, prefix='_', base_table=qb)
        else:
            new_table = self.execute(qb, schema=schema, prefix='_')
            return new_table

    def createEmptyTable(self, schema, prefix='_', pks=None):
        records = [{s: DTMAP[schema[s]] for s in schema}]
        qb = self.insertFromJson(schema=schema, records=records, prefix=prefix, execute=False) \
            .XcalarApiFilter('eq(1,2)')
        if pks:
            qb.XcalarApiSort({k: 'PartialAscending' for k in pks})
        return self.execute(qb, desc='create empty table', prefix=prefix)

    def insertFromJson(self,
                       schema=None,
                       records=[],
                       prefix='_',
                       base_table=None,
                       execute=True,
                       pin_results=False):
        """
        insert table x values ...
        update table y set status = zz where ...
        Create new table from records
        or insert records into existing table
        """
        if type(base_table) == QueryBuilder:
            qb = base_table
            schema = qb.schema
            base_table_in = qb.getTempTableName()
            same_session = True
        elif type(base_table) == str:
            schema = self.get_table(base_table).schema
            qb = QueryBuilder(schema)
            same_session = False
        else:
            if not schema:
                schema = {}
                for f in records[0]:
                    fType = type(records[0][f])
                    if fType == str:
                        try:
                            fType = type(ast.literal_eval(records[0][f]))
                        except Exception:
                            fType = str
                    if fType in [int, bool]:
                        sType = 'int'
                    elif fType == float:
                        sType = 'float'
                    else:
                        sType = 'string'
                    schema[f] = sType
            qb = QueryBuilder(schema)
        key = self.send_2kvstore(json.dumps(records))
        randnum = random.randint(0, 6619)
        parserArg = {'key': key, 'xpu': randnum}
        logger.debug(
            f'Use these args to test dml_insert:insert UDF in XD: {json.dumps(parserArg)}'
        )
        delta_table = qb.XcalarApiBulkLoad(targetName=self.targetName,
                                           parserArg=parserArg,
                                           parserFnName='dml_insert:insert',
                                           path='1') \
            .XcalarApiSynthesize(columns=qb.synthesizeColumns(fromFP=True)).getTempTableName()
        if type(base_table) == QueryBuilder:
            qb.XcalarApiUnion(delta_table, table_in=base_table_in)
        elif type(base_table) == str:
            qb.XcalarApiSynthesize(table_in=base_table, sameSession=same_session) \
                .XcalarApiUnion(delta_table)
        if execute:
            return self.execute(qb, prefix=prefix, pin_results=True)
        else:
            return qb

    def getNumXpus(self):
        qb = QueryBuilder({'xpus':'int'})
        delta_table = qb.XcalarApiBulkLoad(targetName=self.targetName,
                                        #    parserArg=parserArg,
                                           parserFnName='dml_insert:get_xpus',
                                           path='1') \
            .XcalarApiSynthesize(columns=qb.synthesizeColumns(fromFP=True)).getTempTableName()
        tbl_name = self.execute(qb, prefix='xpus', pin_results=True)
        out = self.get_table(tbl_name).records()
        return list(out)[0]['xpus']
