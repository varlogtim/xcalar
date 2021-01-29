import json

import xcalar.compute.util.imd.imd_constants as ImdConstant
import xcalar.external.dataflow


def _get_xcalar_schema(schema, add_meta_cols=False):
    schema = list(schema)
    if add_meta_cols:
        meta_schema = [{
            ImdConstant.Col_name: ImdConstant.Opcode,
            ImdConstant.Col_type: "INTEGER"
        },
                       {
                           ImdConstant.Col_name: ImdConstant.Rankover,
                           ImdConstant.Col_type: "INTEGER"
                       }]
        schema += meta_schema
    schema_json_array = []
    for col in schema:
        col_info = {
            'sourceColumn':
                col[ImdConstant.Col_name],
            'destColumn':
                col[ImdConstant.Col_name],
            'columnType':
                ImdConstant.Table_col_types_map[col[ImdConstant.Col_type]]
        }
        schema_json_array.append(col_info)
    return schema_json_array


def construct_custom_dataflow(client, data_path, table_info, target_name,
                              reason, **kwargs):
    add_meta_cols = False
    schema = table_info["schema"]
    pkeys = table_info.get("primary_keys", [])
    if reason == "checksum":
        parser_name = 'default:parseCsv'
        parser_args = {
            "recordDelim": "",
            "fieldDelim": "",
            "isCRLF": False,
            "linesToSkip": 0,
            "quoteDelim": '"',
            "hasHeader": False,
            "schemaFile": "",
            "schemaMode": "loadInput"
        }
        file_pattern = '*.checksum'
    elif reason == "emptyTable":
        parser_name = 'default:genTableUdf'
        parser_args = {"schema": schema, "records": []}
        file_pattern = ''
    elif reason == "snapshot" or reason == "delta":
        add_meta_cols = reason == "delta"
        if ImdConstant.Export_driver == 'snapshot_parquet':
            parser_name = 'default:parseParquet'
            parser_args = {"parquetParser": "native"}
            file_pattern = '*.parquet'
        else:
            assert ImdConstant.Export_driver == 'snapshot_csv'
            parser_name = 'default:parseCsv'
            parser_args = {"dialect": "xcalarSnapshot"}
            file_pattern = '*.csv.gz'
    else:
        raise ValueError("Supported formats are checksum and emptyTable")

    schema_json_array = _get_xcalar_schema(schema, add_meta_cols)
    dataflow_column_info = [{
        'columnName': col['destColumn']
    } for col in schema_json_array]

    prefix = "p"
    json_query_list = []

    load_query = {
        'operation': 'XcalarApiBulkLoad',
        'comment': '',
        'tag': reason,
        'args': {
            'dest': '.XcalarDS.tab1',
            'loadArgs': {
                'sourceArgsList': [{
                    'targetName': target_name,
                    'path': data_path,
                    'fileNamePattern': file_pattern,
                    'recursive': False
                }],
                'parseArgs': {
                    'parserFnName': parser_name,
                    'parserArgJson': json.dumps(parser_args),
                    'fileNameFieldName': '',
                    'recordNumFieldName': '',
                    'allowFileErrors': False,
                    'allowRecordErrors': False,
                    'schema': schema_json_array
                },
                'size':
                    10737418240
            }
        },
        'annotations': {}
    }
    json_query_list.append(load_query)

    table_query = {
        'operation': 'XcalarApiSynthesize',
        'args': {
            'source': '.XcalarDS.tab1',
            'dest': 'tab2',
            'columns': schema_json_array,
            'sameSession': True,
            'numColumns': 1
        },
        'tag': reason
    }
    json_query_list.append(table_query)

    if pkeys:
        keys_info = []
        keys_type = [
            ImdConstant.Table_col_types_map[col[ImdConstant.Col_type]]
            for col in schema if col[ImdConstant.Col_name] in pkeys
        ]
        for key, key_type in zip(pkeys, keys_type):
            keys_info.append({
                "name": key,
                "ordering": "PartialAscending",
                "keyFieldName": key,
                "type": key_type
            })
        index_query = {
            "operation": "XcalarApiIndex",
            "args": {
                "source": "tab2",
                "dest": "tab3",
                "prefix": "",
                "key": keys_info
            }
        }
        json_query_list.append(index_query)

    query_string = json.dumps(json_query_list)
    df = xcalar.external.dataflow.Dataflow.create_dataflow_from_query_string(
        client, query_string, dataflow_column_info)
    return df
