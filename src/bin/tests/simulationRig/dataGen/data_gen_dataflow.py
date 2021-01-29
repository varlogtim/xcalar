import json

table_col_types_map = {
    "STRING": "DfString",
    "INTEGER": "DfInt64",
    "INT": "DfInt64",
    "BOOLEAN": "DfBoolean",
    "FLOAT": "DfFloat64",
    "TIMESTAMP": "DfTimespec",
    "MONEY": "DfMoney"
}


def data_gen_dataflow(schema, num_records, mem_target, ingest_func_name,
                      ingest_params, export_driver_name, export_params):
    schema_json_array = []
    export_schema_array = []
    export_schema_array1 = []
    for col_name, col_type in schema.items():
        col_name = col_name.upper()
        col_info = {
            'sourceColumn': col_name,
            'destColumn': col_name,
            'columnType': table_col_types_map[col_type.upper()]
        }
        schema_json_array.append(col_info)
        export_schema_array1.append({
            'headerName': col_name,
            'columnName': col_name
        })
        export_schema_array.append({
            'columnName': col_name,
            'headerAlias': col_name
        })

    load_query = {
        'operation': 'XcalarApiBulkLoad',
        'comment': '',
        'tag': '',
        'args': {
            'dest': '.XcalarDS.Optimized.52065.admin.52563.genData',
            'loadArgs': {
                'sourceArgsList': [{
                    'targetName': mem_target,
                    'path': str(num_records),
                    'fileNamePattern': '',
                    'recursive': False
                }],
                'parseArgs': {
                    'parserFnName': ingest_func_name,
                    'parserArgJson': json.dumps(ingest_params),
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

    index_query = {
        'operation': 'XcalarApiSynthesize',
        'args': {
            'source': '.XcalarDS.Optimized.52065.admin.52563.genData',
            'dest': 'genData_1',
            'columns': schema_json_array,
            'sameSession': True,
            'numColumns': 1
        },
        'tag': ''
    }

    export_query = {
        'operation': 'XcalarApiExport',
        'args': {
            'source': 'genData_1',
            'dest': '.XcalarLRQExport.genData_1',
            'columns': export_schema_array1,
            'driverName': export_driver_name,
            'driverParams': json.dumps(export_params)
        },
        'annotations': {}
    }

    dataflow_info = {}
    dataflow_info["query"] = json.dumps(
        [load_query, index_query, export_query])
    dataflow_info["tables"] = [{
        'name': 'genData_1',
        'columns': export_schema_array
    }]
    optimized_query = {'retina': json.dumps(dataflow_info)}
    return dataflow_info["query"], json.dumps(optimized_query)
