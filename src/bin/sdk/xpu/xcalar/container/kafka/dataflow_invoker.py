import json
from .query_builder import QueryBuilder
import xcalar.container.context as ctx
from datetime import datetime
import uuid
import traceback
from xcalar.external.dataflow import Dataflow

logger = ctx.get_logger()


class DataflowInvoker():
    def __init__(self, xcalarClient, session, xcalarApi, xpu_id):
        self.xcalarApi = xcalarApi
        self.session = session
        self.xpu_id = xpu_id
        self.xcalarClient = xcalarClient

    def submit_query(self, qb, out_table, topic_name, pin=False):
        query_name = f'{topic_name}_dfi_{datetime.utcnow().strftime("%s.%f")}'
        query = json.dumps(qb.getQuery(out_table=out_table), indent=4)
        columnsOut = [{'columnName': c, 'headerAlias': c} for c in qb.schema]
        df = Dataflow.create_dataflow_from_query_string(
            self.xcalarClient,
            query_string=query,
            columns_to_export=columnsOut,
            dataflow_name=query_name)
        df_name = self.session.execute_dataflow(
            df,
            table_name=out_table,
            optimized=True,
            is_async=False,
            pin_results=pin)
        logger.info(f'Finished running {df_name}')

    def execute_udf(self, topic_name, udf_name, schema, parserArg, alias=None):
        tempTablePrefix = f'{topic_name}_temp_{uuid.uuid1()}'.upper()
        if alias is None:
            data_table = topic_name.upper()
        else:
            data_table = alias.upper()
        session_tables = self.session.list_tables(data_table)
        logger.info(
            f'Session tables: {list(map(lambda t: t.name, session_tables))}')
        if len(session_tables) == 0:
            logger.info(
                f'Creating base table with schema: {json.dumps(schema)}')
            qb = QueryBuilder(schema=schema, tempTablePrefix=tempTablePrefix)
            logger.info(f'Schema Keys: {schema.keys()}')
            qb.XcalarApiBulkLoad(
                targetName='kafka-base',
                parserArg={'opts': parserArg},
                path='1',
                parserFnName='{}:parse_kafka_topic'.format(udf_name))\
                .XcalarApiSynthesize(columns=qb.synthesizeColumns(fromFP=True))\
                .XcalarApiSort({k: 'PartialAscending' for k in schema.keys()})
            logger.info(f'Using data_table: {data_table}')
            self.submit_query(qb, data_table, topic_name, pin=True)
            t = self.session.get_table(data_table)
            fqn = t.publish()
            logger.info(f'Published {t.name} to global namespace as {fqn}')
            return data_table
        else:
            logger.info(f'Reading deltas and appending to base')
            qb = QueryBuilder(
                schema={
                    **schema, 'XcalarOpCode': 'int',
                    'XcalarRankOver': 'int'
                },
                tempTablePrefix=tempTablePrefix)
            qb.XcalarApiBulkLoad(
                targetName='kafka-base',
                parserArg={'opts': parserArg},
                path='1',
                parserFnName='{}:parse_kafka_topic'.format(udf_name))\
                .XcalarApiSynthesize(columns=qb.synthesizeColumns(fromFP=True))\
                .XcalarApiSort({k: 'PartialAscending' for k in schema.keys()})\
                .XcalarApiMap({'XcalarOpCode': 'absInt(1)', 'XcalarRankOver': 'absInt(1)'})
            if alias is None:
                delta_table = f'{topic_name}_delta_{uuid.uuid1()}'.upper()
            else:
                delta_table = f'{alias}_delta_{uuid.uuid1()}'.upper()
            logger.info(
                f'Using data_table: {data_table}, delta_table: {delta_table}')
            self.submit_query(qb, delta_table, topic_name)
            self.merge(data_table, delta_table)

    def merge(self, data_table, delta_table):
        try:
            t1 = self.session.get_table(data_table)
            t2 = self.session.get_table(delta_table)
            t2_record_count = t2.record_count()
            # TODO Log at debug for performance
            logger.info(
                f'Merging {t1.name}::{t1.record_count()} with {t2.name}::{t2_record_count}'
            )
            logger.info(f't1: {t1.schema}, t2: {t2.schema}')
            if t2_record_count > 0:
                t1.merge(t2)
            # TODO Log at debug for performance
            logger.info(f'Post-merge {t1.name}::{t1.record_count()}')
            return delta_table
        finally:
            self.session.get_table(delta_table).drop(delete_completely=True)

    def purge(self, topic_name, schema, purge_interval_in_seconds, alias=None):
        tempTablePrefix = f'{topic_name}_temp_{uuid.uuid1()}'.upper()
        if alias is None:
            data_table = topic_name.upper()
        else:
            data_table = alias
        try:
            # purge_timestamp = purge_time.strftime('%Y-%m-%d %-H:%M:%S')
            purge_timestamp = datetime.utcnow().timestamp(
            ) - purge_interval_in_seconds    # Timestamp purge_time seconds older
            logger.info(
                f'Purging rows older than {purge_timestamp} from {data_table} with schema: {json.dumps(schema)}'
            )
            filter_eval = f"lt(ts_nanos,{purge_timestamp*1000*1000*1000})"    # Use nanos
            sortkeys = {k: 'PartialAscending' for k in schema.keys()}
            qb = QueryBuilder(
                schema={
                    **schema, 'XcalarOpCode': 'int',
                    'XcalarRankOver': 'int'
                },
                tempTablePrefix=tempTablePrefix)
            qb.XcalarApiSynthesize(table_in=data_table, sameSession=False, columns=qb.synthesizeColumns()) \
                .XcalarApiFilter(filter_eval) \
                .XcalarApiSort(sortkeys) \
                .XcalarApiMap({'XcalarOpCode': 'absInt(0)', 'XcalarRankOver': f'absInt(1)'})
            delta_table = f'{topic_name}_purge_{uuid.uuid1()}'.upper()
            self.submit_query(qb, delta_table, topic_name)
            self.merge(data_table, delta_table)
            logger.info(f'Purged {data_table} successfully')
        except Exception:
            logger.warn(
                f'Failed to purge {data_table}, error: {traceback.format_exc()}'
            )
