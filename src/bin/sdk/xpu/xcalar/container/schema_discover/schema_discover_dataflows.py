import json

# This is the crufiest of the files in the commit. This is mostly
# due to the lack of a robust dataflow SDK ... Improve me later.

ICV_COLUMN_NAME = 'XCALAR_ICV'
FRN_COLUMN_NAME = 'XCALAR_FILE_RECORD_NUM'
SRC_COLUMN_NAME = 'XCALAR_SOURCEDATA'
PATH_COLUMN_NAME = 'XCALAR_PATH'
INDEX_COLUMN_NAME = 'XcalarRankOver'

PUBLISH_SCHEMA = [{'name': INDEX_COLUMN_NAME, 'type': 'DfInt64'}]
S3_PARSER_SCHEMA = [{
    'name': ICV_COLUMN_NAME,
    'type': 'DfString'
}, {
    'name': FRN_COLUMN_NAME,
    'type': 'DfInt64'
}, {
    'name': SRC_COLUMN_NAME,
    'type': 'DfString'
}, {
    'name': PATH_COLUMN_NAME,
    'type': 'DfString'
}]
DATA_COLS_TO_KEEP = [PATH_COLUMN_NAME, INDEX_COLUMN_NAME]
ICV_COLS_TO_KEEP = [c['name'] for c in S3_PARSER_SCHEMA] + [INDEX_COLUMN_NAME]


def get_df_schemas(schema):
    synthesize_op_schema = []
    export_op_schema = []
    df_tables_schema = []

    for cc in schema:
        synthesize_op_schema.append({
            'sourceColumn': cc['name'],
            'destColumn': cc['name'],
            'columnType': cc['type']
        })
        export_op_schema.append({
            'columnName': cc['name'],
            'headerName': cc['name']
        })
        df_tables_schema.append({
            'columnName': cc['name'],
            'headerAlias': cc['name']
        })
    return (synthesize_op_schema, export_op_schema, df_tables_schema)


def get_load_table_query_string(source_args, parser_fn_name, parser_arg_json,
                                schema, table_name):

    combined_schema = schema + S3_PARSER_SCHEMA
    combined_published_schema = combined_schema + PUBLISH_SCHEMA

    export_driver = 'do_nothing'
    export_params = {}

    (synth_op_schema, _, df_tables_schema) = get_df_schemas(combined_schema)
    (_, export_op_schema, _) = get_df_schemas(combined_published_schema)

    dataset_name = f'.XcalarDS.Optimized.12345.admin.12345.{table_name}'

    load_op = {
        'operation': 'XcalarApiBulkLoad',
        'comment': '',
        'tag': '',
        'args': {
            'dest': dataset_name,
            'loadArgs': {
                'sourceArgsList': source_args,
                'parseArgs': {
                    'parserFnName': f'{parser_fn_name}',
                    'parserArgJson': parser_arg_json,
                    'schema': synth_op_schema,
                    'fileNameFieldName': '',
                    'recordNumFieldName': '',
                    'allowFileErrors': False,
                    'allowRecordErrors': False,
                },
                'size': 10737418240
            }
        },
        'annotations': {}
    }

    synth_op = {
        'operation': 'XcalarApiSynthesize',
        'args': {
            'source': dataset_name,
            'dest': f'{table_name}_synth',
            'columns': synth_op_schema,
            'sameSession': True,
            'numColumns': 1
        },
        'tag': ''
    }

    row_num_op = {
        'operation': 'XcalarApiGetRowNum',
        'args': {
            'source': f'{table_name}_synth',
            'dest': f'{table_name}_rownum',
            'newField': 'XcalarRankOver'
        },
        'state': 'Dropped'
    }

    index_op = {
        'operation': 'XcalarApiIndex',
        'args': {
            'source':
                f'{table_name}_rownum',
            'dest':
                f'{table_name}',
            'broadcast':
                False,
            'delaySort':
                False,
            'dhtName':
                '',
            'key': [{
                'name': 'XcalarRankOver',
                'type': 'DfInt64',
                'keyFieldName': 'XcalarRankOver',
                'ordering': 'Unordered'
            }],
            'prefix':
                ''
        },
        'state': 'Dropped'
    }

    export_op = {
        'operation': 'XcalarApiExport',
        'args': {
            'source': f'{table_name}',
            'dest': f'.XcalarLRQExport.{table_name}',
            'columns': export_op_schema,
            'driverName': 'do_nothing',
            'driverParams': '{}'
        },
        'annotations': {}
    }

    dataflow_info = {}

    query = [load_op, synth_op, row_num_op, index_op, export_op]
    query_json = json.dumps(query)
    dataflow_info["query"] = query_json
    dataflow_info["tables"] = [{
        'name': table_name,
        'columns': df_tables_schema
    }]
    optimized_query = {'retina': json.dumps(dataflow_info)}

    query_string = dataflow_info['query']
    optimized_query_string = json.dumps(optimized_query)

    return (query_string, optimized_query_string)
