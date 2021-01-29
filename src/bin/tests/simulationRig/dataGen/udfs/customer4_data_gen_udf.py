import json
import random
import time
import datetime
import string
import os
import logging

from xcalar.compute.coretypes.DataFormatEnums.ttypes import DfFormatTypeT

from xcalar.external.client import Client
import xcalar.container.table
try:
    import xcalar.container.cluster
    cluster = xcalar.container.cluster.get_running_cluster()
except Exception:
    pass

INTEGER = ['integer', 'int']
FLOAT = ['float']
STRING = ['string']
TIMESTAMP = ['timestamp']

Max_num = 10000000000

p_keys = None
'''
Notes:
1) Divide the unique values among xpus to generate in its range
2) Have a primary-key and foreign key mapping and select numbers from the same population for both, do this based on the join results after some experiments
3)
'''

logger = logging.getLogger('xcalar')
logger.setLevel(logging.INFO)


class DataGen:
    def __init__(self, col_name, data_type, start_num, row_count):
        logger.info('start_num: {}, row_count: {}'.format(
            start_num, row_count))
        self.col_name = col_name
        self.data_type = data_type
        self.start_num = start_num
        self.row_count = row_count

    def get_value(self):
        logger.info('-> start_num: {}, row_count: {}'.format(
            self.start_num, self.row_count))
        while True:
            val = random.randint(self.start_num, self.row_count)
            if self.data_type == STRING:
                yield '{}'.format(val)
            elif self.data_type in INTEGER:
                yield val
            elif self.data_type in FLOAT:
                yield val + random.uniform(0, 1)
            elif self.data_type in TIMESTAMP:
                yield datetime.datetime.now().timestamp() * 1000
            else:
                yield None


'''
bucket_stats
"stats": [
          {
            "MAX": 48,
            "MIN": 0,
            "COUNT": 25,
            "BUCKET": 1
          }
    ]
'''


class DataGenContinuous(DataGen):
    def __init__(self, col_name, data_type, start_num, row_count, stats):
        super(DataGenContinuous, self).__init__(col_name, data_type, start_num,
                                                row_count)
        bucket_stats = stats['stats']
        self.bucket_cuml_weights = []
        self.bucket_min_max = []
        for bucket in bucket_stats:
            self.bucket_cuml_weights.append(bucket['COUNT'])
            min_val = bucket.get('MIN', None)
            max_val = bucket.get('MAX', None)
            self.bucket_min_max.append((min_val, max_val))
        # these asserts are valid for customer4 schema
        assert (len(self.bucket_min_max) > 0)

    def get_value(self):
        logger.debug('col name: {}, weights: {}'.format(
            self.col_name, self.bucket_cuml_weights))
        while True:
            bucket_min, bucket_max = random.choices(
                self.bucket_min_max, weights=self.bucket_cuml_weights)[0]
            if bucket_min is None and bucket_max is None:
                yield None
                continue
            if bucket_min is None:
                bucket_min = bucket_max - random.randint(1, 1000)
            else:
                bucket_max = bucket_min + random.randint(1, 1000)
            if self.data_type in INTEGER:
                assert bucket_min is not None
                assert bucket_max is not None
                yield random.randint(bucket_min, bucket_max)
            else:
                assert bucket_min is not None
                assert bucket_max is not None
                yield random.uniform(bucket_min, bucket_max)


class DataGenCategorical(DataGen):
    def __init__(self, col_name, data_type, start_num, row_count, stats,
                 stats_row_count):
        super(DataGenCategorical, self).__init__(col_name, data_type,
                                                 start_num, row_count)
        self.weights = []
        self.col_vals = []
        col_stats = stats['stats']
        values_hidden = stats.get('values_hidden', False)
        scale_fn = lambda x: (row_count * x) // stats_row_count    # NOQA
        # unique vals relative to user specified row count
        self.unique_vals = scale_fn(stats['unique_values'])
        if self.unique_vals == 0:
            self.unique_vals = stats['unique_values']
        if col_stats != 'hidden':
            for stat in col_stats:
                # count relative to user specified row count
                count = scale_fn(stat['COUNT'])
                count = 1 if count == 0 else count
                self.weights.append(count)
                if values_hidden:
                    val = ''.join(
                        [random.choice(string.digits) for _ in range(8)])
                else:
                    val = stat.get(self.col_name, None)
                self.col_vals.append(val)
        # check if we have to gen more uniq values
        if self.unique_vals > len(self.col_vals):
            # performance is very slow if we consider all unique values let's limit
            # till 1000 or lower
            remaining_unique_vals = min(self.unique_vals - len(self.col_vals),
                                        1000)
            partitioned_uniq_vals = remaining_unique_vals // cluster.total_size + 1
            assert remaining_unique_vals > 0
            logger.info('Input {} {} {}'.format(col_name, data_type,
                                                remaining_unique_vals))

            string_lens_collection = None
            if 'string-lengths' in stats:
                string_lens_collection = stats['string-lengths']
            else:
                string_lens_collection = [{'LENGTH': x} for x in range(1, 5)]
            num_input_str_lengths = len(string_lens_collection)
            unique_count = (
                row_count - sum(self.weights)) // remaining_unique_vals + 1
            unique_count = 1 if unique_count <= 0 else unique_count
            # xpu's work independently by generating the values in their partition range
            while partitioned_uniq_vals > 0:
                str_len_input = string_lens_collection[
                    (partitioned_uniq_vals + cluster.my_xpu_id) %
                    num_input_str_lengths]
                str_len = str_len_input.get('LENGTH', 0)
                self.weights.append(unique_count)
                if self.col_name.lower() in p_keys:
                    val = ''.join(
                        [random.choice(string.digits) for _ in range(str_len)])
                else:
                    val = ''.join([
                        random.choice(string.ascii_letters)
                        for _ in range(str_len)
                    ])
                self.col_vals.append(val)
                partitioned_uniq_vals -= 1
        logger.debug('col name {}'.format(self.col_name))
        logger.debug('values {}'.format(self.col_vals))
        logger.debug('weigths {}'.format(self.weights))
        # these asserts are valid for customer4 schema
        assert (len(self.col_vals) > 0)
        assert len(self.col_vals) == len(self.weights)

        # XXX add user hardcoded values probablities here

    def get_value(self):
        logger.debug('col name: {}, vals: {}, weights: {}'.format(
            self.col_name, self.col_vals, self.weights))
        while True:
            yield random.choices(self.col_vals, weights=self.weights)[0]


class DataGenLookupXcalarTable(DataGen):
    def __init__(self, col_name, data_type, row_count, stats, stats_row_count,
                 ref_table_name, ref_col_name, is_pkey, user_name,
                 session_name):
        super(DataGenLookupXcalarTable, self).__init__(col_name, data_type,
                                                       None, None)

        cluster = xcalar.container.cluster.get_running_cluster()
        # take master xpu of node 0
        node_0_master = cluster.local_master_for_node(0)
        if cluster.my_xpu_id == node_0_master:
            # assuming, session is present on master node
            client = Client(bypass_proxy=True, user_name=user_name)
            sess = client.get_session(session_name)
            tab = sess.get_table("{}_pkeys".format(ref_table_name))
            xdb_id = tab._get_meta().xdb_id
            tab_info = {
                'xdb_id':
                    xdb_id,
                'columns': [{
                    'columnName': ref_col_name,
                    'headerAlias': ref_col_name
                }]
            }
            cluster.broadcast_msg(tab_info)
        else:
            tab_info = cluster.recv_msg()
        self.table = xcalar.container.table.Table(tab_info['xdb_id'],
                                                  tab_info['columns'])
        logger.info('{}:{}:{}:{}'.format(col_name, data_type, ref_table_name,
                                         ref_col_name))
        self.ref_col_name = ref_col_name
        self.is_pkey = is_pkey
        self.tab_row_generator = self.table.partitioned_rows(
            format=DfFormatTypeT.DfFormatInternal,
            field_delim=',',
            record_delim='\n',
            quote_delim='"')
        if not is_pkey:
            records = [row.strip('\n') for row in self.tab_row_generator]
            assert len(records) > 0
            self.col_vals = []
            self.weights = []
            col_stats = stats['stats']
            values_hidden = stats.get('values_hidden', False)
            scale_fn = lambda x: (row_count * x) // stats_row_count    # NOQA
            if data_type in STRING:
                # unique vals relative to user specified row count
                unique_vals = scale_fn(stats['unique_values'])
                if unique_vals == 0:
                    unique_vals = stats['unique_values']
                if col_stats != 'hidden':
                    for stat in col_stats:
                        # count relative to user specified row count
                        count = scale_fn(stat['COUNT'])
                        count = 1 if count == 0 else count
                        self.weights.append(count)
                        if values_hidden:
                            val = random.choice(records)
                            records.remove(val)
                        else:
                            val = stat.get(self.col_name, None)
                            if val and records and val not in records:
                                val = random.choice(records)
                                records.remove(val)
                        self.col_vals.append(val)
                        if not records:
                            break
                if unique_vals > len(self.col_vals):
                    remaining_unique_vals = min(
                        unique_vals - len(self.col_vals), 1000)
                    # treat all remaining equal
                    unique_count = (row_count - sum(
                        self.weights)) // remaining_unique_vals + 1
                    unique_count = 1 if unique_count <= 0 else unique_count
                    self.col_vals += list(records)
                    self.weights += [unique_count] * len(records)
                    del records
            else:
                self.col_vals += list(records)
                self.weights += [1] * len(records)
                del records
            assert len(self.col_vals) == len(
                self.weights) and len(self.col_vals) > 0, '{}:{}'.format(
                    self.col_vals, self.weigths)

    def get_value(self):
        while True:
            if self.is_pkey:
                for row in self.tab_row_generator:
                    yield row.strip('\n')
                self.tab_row_generator = self.table.partitioned_rows(
                    format=DfFormatTypeT.DfFormatInternal,
                    field_delim=',',
                    record_delim='\n',
                    quote_delim='"')
            else:
                yield random.choices(self.col_vals, weights=self.weights)[0]


def gen_customer4_data(filepath, instream, schema_inputs):
    global p_keys
    inObj = json.loads(instream.read())
    if inObj['numRows'] == 0:
        raise ValueError(
            'Insufficent num of records requested; should be atleast num cores of cluster'
        )
    start = inObj['startRow'] + 1
    end = start + inObj['numRows']

    seed = schema_inputs.get('seed', int(time.time())) + start
    path_to_schemas = schema_inputs.get('path_to_schemas')
    path_to_data_stats = schema_inputs.get('path_to_data_stats')
    path_to_contraints = schema_inputs.get('path_to_contraints', None)
    join_keys_only = schema_inputs.get('join_keys_only', False)
    user_name = schema_inputs.get('user_name')
    session_name = schema_inputs.get('session_name')
    table_name = schema_inputs.get('table_name')
    batch_id = int(schema_inputs.get('batch_id'))
    total_row_count = schema_inputs.get('total_row_count')

    random.seed(seed)
    start_num = start
    if batch_id > 1:
        start_num = batch_id * inObj['numRows'] + random.randint(1, Max_num)

    schema = None
    with open(path_to_schemas, 'r') as schema_file:
        schemas = json.load(schema_file)
    data_stats = {}
    if os.path.exists(path_to_data_stats):
        with open(path_to_data_stats, 'r') as stats_file:
            data_stats = json.load(stats_file)
    tab_constraints = {}
    pkeys_constr = []
    fkeys_constr = {}
    if path_to_contraints:
        with open(path_to_contraints, 'r') as join_keys_file:
            tab_constraints = json.load(join_keys_file).get(table_name, {})
            assert len(tab_constraints) > 0, str(tab_constraints)
        if tab_constraints:
            pkeys_constr = tab_constraints.get('pkeys', [])
            fkeys_constr = tab_constraints.get('fkeys', {})

    schema = {}
    columns_stats = data_stats.get('columns', {})
    p_keys = schemas[table_name]['keys']
    column_names = []
    value_generators = []
    for col_name, col_type in schemas[table_name]['columns'].items():
        col_name_upper = col_name.upper()
        if join_keys_only and col_name_upper not in pkeys_constr:
            continue
        schema[col_name_upper] = {}
        stats = columns_stats.get(col_name_upper, None)
        schema[col_name_upper]['COL_TYPE'] = col_type
        generator = None
        if col_name_upper in fkeys_constr:
            stats_row_count = data_stats.get('row_count')
            ref_table_name, ref_col_name = fkeys_constr[col_name_upper].split(
                ':')
            generator = DataGenLookupXcalarTable(
                col_name_upper,
                col_type,
                total_row_count,
                stats,
                stats_row_count,
                ref_table_name,
                ref_col_name,
                False,
                user_name=user_name,
                session_name=session_name)
        elif not join_keys_only and col_name_upper in pkeys_constr:
            stats_row_count = data_stats.get('row_count')
            generator = DataGenLookupXcalarTable(
                col_name_upper,
                col_type,
                total_row_count,
                stats,
                stats_row_count,
                table_name,
                col_name_upper,
                True,
                user_name=user_name,
                session_name=session_name)
        elif stats is None:
            generator = DataGen(col_name_upper, col_type, start_num,
                                total_row_count)
        # if unique values key exists it is more like a string type
        elif col_type in STRING or 'unique_values' in stats:
            stats_row_count = data_stats.get('row_count')
            generator = DataGenCategorical(col_name_upper, col_type, start_num,
                                           total_row_count, stats,
                                           stats_row_count)
        elif col_type in INTEGER or col_type in FLOAT:
            generator = DataGenContinuous(col_name_upper, col_type, start_num,
                                          total_row_count, stats)
        else:
            logger.info('col_type: {}, stats: {}'.format(col_type, stats))
            assert False, 'not implemented error'
        column_names.append(col_name_upper)
        value_generators.append(generator.get_value())

    entry_dt = datetime.datetime.now().timestamp() * 1000
    for column_values in zip(*value_generators):
        rec = dict(zip(column_names, column_values))
        rec['BATCH_ID'] = batch_id
        rec['ENTRY_DT'] = entry_dt
        yield rec
        start += 1
        if start >= end:
            break
