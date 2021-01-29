#
# Build Query string of FASJ operators
#

import json
from xcalar.compute.coretypes.JoinOpEnums.ttypes import JoinOperatorT
from xcalar.compute.coretypes.JoinOpEnums.constants import JoinOperatorTStr
from xcalar.compute.coretypes.UnionOpEnums.ttypes import UnionOperatorT
from xcalar.compute.coretypes.UnionOpEnums.constants import UnionOperatorTStr
from xcalar.compute.coretypes.OrderingEnums.constants import XcalarOrderingTStr
from xcalar.compute.coretypes.DataFormatEnums.constants import DfFieldTypeTStr


#
# XXX TODO
# * Add help
# * Add better arg checks to prevent parser failure downstream
#
class QueryGenerate:
    def __init__(self, query=[]):
        self.query = query

    @property
    def get_query(self):
        return self.query

    @property
    def get_query_string(self):
        return json.dumps(list(self.query))

    def get_table_rename_maps(self, source_columns, dest_columns,
                              column_types):
        rename_map = []
        for src_col, dst_col, col_type in zip(source_columns, dest_columns,
                                              column_types):
            col_map = {}
            col_map['sourceColumn'] = src_col
            col_map['destColumn'] = dst_col
            col_map['columnType'] = col_type
            rename_map.append(col_map)
        return rename_map

    def get_key_maps(self, key_names, key_field_names, key_types,
                     key_orderings):
        key_map = []
        for name, field_name, field_type, ordering in zip(
                key_names, key_field_names, key_types, key_orderings):
            col_map = {}
            col_map['name'] = name
            col_map['keyFieldName'] = field_name
            col_map['type'] = DfFieldTypeTStr[field_type]
            col_map['ordering'] = XcalarOrderingTStr[ordering]
            key_map.append(col_map)
        return key_map

    def op_aggregate(self, source, dest, eval_string):
        self.query.append({
            'operation': 'XcalarApiAggregate',
            'args': {
                'source': source,
                'dest': dest,
                'eval': [{
                    'evalString': eval_string
                }]
            }
        })

    def get_bulk_load_source_args(self, target_name, path, file_name_pattern,
                                  recursive):
        source_args = {
            'targetName': target_name,
            'path': path,
            'fileNamePattern': file_name_pattern,
            'recursive': recursive
        }
        return source_args

    def get_bulk_load_source_args_list(self, target_name_list, path_list,
                                       file_name_pattern_list, recursive_list):
        source_args_list = []
        for target_name, path, file_name_pattern, recursive in zip(
                target_name_list, path_list, file_name_pattern_list,
                recursive_list):
            source_args_list.append(
                self.get_bulk_load_source_args(target_name, path,
                                               file_name_pattern, recursive))
        return source_args_list

    def get_parse_args(self,
                       parser_fn_name,
                       parser_arg_json,
                       file_name_field_name,
                       record_num_field_name,
                       allow_file_errors=False,
                       allow_record_errors=False,
                       schema={}):
        parse_args = {
            'parseFnName': parser_fn_name,
            'parseArgJson': json.dumps(parser_arg_json),
            'fileNameFieldName': file_name_field_name,
            'recordNumFieldName': record_num_field_name,
            'allowFileErrors': allow_file_errors,
            'allow_record_errors': allow_record_errors,
            'schema': schema
        }
        return parse_args

    def op_bulk_load(self,
                     dest,
                     keys_map,
                     filter_string,
                     source_args_list,
                     parse_args,
                     sample_size,
                     source_args=None):
        xdb_args = {}
        xdb_args['key'] = keys_map
        xdb_args['evalString'] = filter_string
        self.query.append({
            'operation': 'XcalarApiBulkLoad',
            'args': {
                'dest': dest,
                'loadArgs': {
                    'sourceArgs': source_args,
                    'sourceArgsList': source_args_list,
                    'parseArgs': parse_args,
                    'size': sample_size
                }
            }
        })

    def op_index(self,
                 source,
                 dest,
                 keys_map,
                 prefix='',
                 dht_name='',
                 delta_sort=False,
                 broadcast=False):
        self.query.append({
            'operation': 'XcalarApiIndex',
            'args': {
                'source': source,
                'dest': dest,
                'key': keys_map,
                'prefix': prefix,
                'dhtName': dht_name,
                'delaySort': False,
                'broadcast': False
            }
        })

    def op_project(self, source, dest, column_names):
        self.query.append({
            'operation': 'XcalarApiProject',
            'args': {
                'source': source,
                'dest': dest,
                'columns': column_names
            }
        })

    def op_row_num(self, source, dest, new_field):
        self.query.append({
            'operation': 'XcalarApiGetRowNum',
            'args': {
                'source': source,
                'dest': dest,
                'newField': new_field
            }
        })

    def op_filter(self, source, dest, eval_string):
        self.query.append({
            'operation': 'XcalarApiFilter',
            'args': {
                'source': source,
                'dest': dest,
                'eval': [{
                    'evalString': eval_string,
                    'newField': None
                }]
            }
        })

    def op_groupby(self,
                   source,
                   dest,
                   eval_list,
                   new_key_field='',
                   keys_map=None,
                   include_sample=False,
                   icv=False,
                   group_all=False):
        if (not isinstance(eval_list, list)):
            raise TypeError("evals_list must be list object, not '{}'".format(
                type(eval_list)))
        try:
            aggr_strs, new_col_names = list(zip(*eval_list))
            assert len(aggr_strs) == len(new_col_names)
        except ValueError:
            raise ValueError("evals_list should be a list of tuple objects")
        except AssertionError as e:
            raise ValueError(
                "evals_list should be list of two element tuple") from e

        self.query.append({
            'operation': 'XcalarApiGroupBy',
            'args': {
                'source':
                    source,
                'dest':
                    dest,
                'eval': [{
                    'evalString': e,
                    'newField': c,
                } for e, c in zip(aggr_strs, new_col_names)],
                'newKeyField':
                    new_key_field,
                'key':
                    keys_map,
                'includeSample':
                    include_sample,
                'icv':
                    icv,
                'groupAll':
                    group_all
            }
        })

    def op_join(self,
                source_left,
                source_right,
                dest,
                left_rename_maps,
                right_rename_maps,
                join_type,
                left_keys=[],
                right_keys=[],
                eval_string='',
                keep_all_columns=False,
                null_safe=False):
        self.query.append({
            'operation': 'XcalarApiJoin',
            'args': {
                'source': [source_left, source_right],
                'dest': dest,
                'joinType': JoinOperatorTStr[join_type],
                'key': [left_keys, right_keys],
                'columns': [left_rename_maps, right_rename_maps],
                'evalString': eval_string,
                'keepAllColumns': keep_all_columns,
                'nullSafe': null_safe
            }
        })

    def op_union(self,
                 sources,
                 dest,
                 table_rename_maps,
                 union_type,
                 keys=[],
                 dedup=False):
        self.query.append({
            'operation': 'XcalarApiUnion',
            'args': {
                'source': sources,
                'dest': dest,
                'dedup': dedup,
                'columns': table_rename_maps,
                'unionType': UnionOperatorTStr[union_type],
                'key': keys
            }
        })

    def op_map(self, source, dest, evals_list, icv=False):
        if (not isinstance(evals_list, list)):
            raise TypeError("evals_list must be list object, not '{}'".format(
                type(evals_list)))
        try:
            map_strs, new_col_names = list(zip(*evals_list))
            assert len(map_strs) == len(new_col_names)
        except ValueError:
            raise ValueError("evals_list should be a list of tuple objects")
        except AssertionError as e:
            raise ValueError(
                "evals_list should be list of two element tuple") from e

        self.query.append({
            'operation': 'XcalarApiMap',
            'args': {
                'source':
                    source,
                'dest':
                    dest,
                'eval': [{
                    'evalString': e,
                    'newField': c,
                } for e, c in zip(map_strs, new_col_names)],
                'icv':
                    icv
            }
        })

    def op_synthesize(self, source, dest, table_rename_map, same_session=True):
        self.query.append({
            'operation': 'XcalarApiSynthesize',
            'args': {
                'source': source,
                'dest': dest,
                'sameSession': same_session,
                'columns': table_rename_map
            }
        })

    def get_file_export_driver_params(self, directory_path, file_base,
                                      field_delim, record_delim, quote_delim):
        driver_params = {
            'directory_path': directory_path,
            'file_base': file_base,
            'field_delim': field_delim,
            'record_delim': record_delim,
            'quote_delim': quote_delim
        }
        return driver_params

    def get_udf_export_driver_params(self, directory_path, export_udf,
                                     file_name, header, field_delim,
                                     record_delim, quote_delim):
        driver_params = {
            'directory_path': directory_path,
            'export_udf': export_udf,
            'file_name': file_name,
            'header': header,
            'fieldDelim': field_delim,
            'recordDelim': record_delim,
            'quoteDelim': quote_delim
        }
        return driver_params

    def op_export(self, source, dest, column_names, header_names, driver_name,
                  driver_params):
        self.query.append({
            'operation': 'XcalarApiExport',
            'args': {
                'source':
                    source,
                'dest':
                    dest,
                'columns': [{
                    'columnName': cn,
                    'headerName': hn
                } for cn, hn in zip(column_names, header_names)],
                'driverName':
                    driver_name,
                'driverParams':
                    json.dumps(driver_params)
            }
        })
