"""
These test generate a variety of datasets, imports them, and calculates
the MiB/second at which we loaded. It is important to note that we are
using the Pickled size in our calculation. These numbers are comparable
between test runs only, i.e., for determining change in performance of
two different versions, not as stand alone statistics.

To run, comment line 31:#pytestmark = pytest.mark.skip
Run with: ./PyTest.sh -s -k test_benchmark_<test> # The -s prints to stdout
"""
import os
import time
import pickle
import random
import tempfile
import pytest
from datetime import datetime

from xcalar.compute.util.Qa import DefaultTargetName

#
# This skips the module since we do not want this to run as part of regular tests
pytestmark = pytest.mark.skip

##
# Globals
##

SEED = 0x0ff5e75

benchmark_udf_name = "benchmarkUdf"
benchmark_udf_source = """
import pickle
def parse(filePath, inStream):
    obj = pickle.load(inStream)
    yield from obj
"""

udfs = [(benchmark_udf_name, benchmark_udf_source)]


@pytest.fixture(scope="module")
def setup(client):
    workbook = client.create_workbook("benchmark_test")

    for udf_name, udf_source in udfs:
        workbook.create_udf_module(udf_name, udf_source)

    yield workbook
    workbook.delete()


##
# Benchmark Functions
##


def _run_benchmark(workbook, data):
    num_tries = 1

    durations = []
    ds_sizes = []
    with tempfile.NamedTemporaryFile('wb', buffering=2**20) as tmpf:
        pickle.dump(data, tmpf)
        tmpf.flush()
        file_size = os.stat(tmpf.name).st_size
        for _ in range(num_tries):
            db = workbook.build_dataset(
                "strings",
                DefaultTargetName,
                tmpf.name,
                "udf",
                parser_name="{}:parse".format(benchmark_udf_name))
            start = time.time()
            dataset = db.load()
            duration = time.time() - start
            durations.append(duration)
            ds_size = dataset.get_info()['datasetSize']
            ds_sizes.append(ds_size)
            dataset.delete()
    avg_duration = sum(durations) / len(durations)
    avg_size = sum(ds_sizes) / len(ds_sizes)
    return {
        'duration': ((file_size / avg_duration) / 2**20),    # MiB/sec
        'size': avg_size
    }


def _run_benchmark_strings(workbook, num_bytes, num_fields, field_size):
    """
    A note about num_bytes: So, num_bytes is used as a "best guess" for how large we
    should make our dataset. The idea is that we want at least 1000 xdb pages used.
    So, we just sort of guess, sometimes better than others, at how large the dataset
    will be.
    """
    single_row = {
        "field {}".format(ii): "T" * field_size
        for ii in range(num_fields)
    }
    row_size = len(single_row) * field_size
    num_rows = num_bytes // row_size
    data = [single_row for _ in range(num_rows)]
    return _run_benchmark(workbook, data)


def test_benchmark_strings(setup):
    workbook = setup

    num_bytes = 200 * 2**20
    num_fields_list = [3**x for x in range(5)]
    field_size_list = [4**x for x in range(5)]

    durations_out = 'Units/sec,' + ','.join([
        "{} byte fields".format(field_size) for field_size in field_size_list
    ]) + "\n"
    sizes_out = 'Dataset Sizes,' + ','.join([
        "{} byte fields".format(field_size) for field_size in field_size_list
    ]) + "\n"

    for num_fields in num_fields_list:
        durations = []
        sizes = []
        for field_size in field_size_list:
            result = _run_benchmark_strings(workbook, num_bytes, num_fields,
                                            field_size)
            durations.append(result['duration'])
            sizes.append(result['size'])
        field_durations = ','.join([str(d) for d in durations])
        durations_out = durations_out + "{} fields,{}\n".format(
            num_fields, field_durations)
        field_sizes = ','.join([str(s) for s in sizes])
        sizes_out = sizes_out + "{} fields,{}\n".format(
            num_fields, field_sizes)
    print('#' * 20 + ' DURATIONS ' + '#' * 20)
    print(durations_out)
    print('#' * 20 + ' DATASET SIZES ' + '#' * 20)
    print(sizes_out)


def _run_benchmark_objs(workbook, num_bytes, num_fields):
    obj = {
        "pi": [3, 1, 4, 1, 5, 9, 2, 6],
        "phi": [1, 6, 1, 8, 0, 3, 3, 9],
        "e": [2, 7, 1, 2, 8, 1, 8, 2],
        "dude": "Maybe throw a string in there..."
    }
    obj_len = sum([len(x) for x in obj])
    single_row = {"field {}".format(ii): obj for ii in range(num_fields)}
    row_size = obj_len * num_fields
    num_rows = num_bytes // row_size
    data = [single_row for _ in range(num_rows)]
    return _run_benchmark(workbook, data)


def test_benchmark_objs(setup):
    workbook = setup

    num_bytes = 200 * 2**20
    num_fields_list = [3**x for x in range(5)]

    print('Objects,Units/Second,Dataset Size')

    for num_fields in num_fields_list:
        result = _run_benchmark_objs(workbook, num_bytes, num_fields)
        duration = result['duration']
        size = result['size']
        print("{} fields,{},{}".format(num_fields, duration, size))


def _run_benchmark_floats(workbook, num_bytes, num_fields):
    single_row = {
        "field {}".format(ii): float(1.61803398875)
        for ii in range(num_fields)
    }
    row_size = len(single_row)
    num_rows = num_bytes // row_size
    data = [single_row for _ in range(num_rows)]
    return _run_benchmark(workbook, data)


def test_benchmark_floats(setup):
    workbook = setup

    num_bytes = 200 * 2**20
    num_fields_list = [3**x for x in range(5)]

    print('Floats,Units/Second,Dataset Size')

    for num_fields in num_fields_list:
        result = _run_benchmark_floats(workbook, num_bytes, num_fields)
        duration = result['duration']
        size = result['size']
        print("{} fields,{},{}".format(num_fields, duration, size))


def _run_benchmark_booleans(workbook, num_bytes, num_fields):
    single_row = {"field {}".format(ii): True for ii in range(num_fields)}
    row_size = len(single_row)
    num_rows = num_bytes // row_size
    data = [single_row for _ in range(num_rows)]
    return _run_benchmark(workbook, data)


def test_benchmark_booleans(setup):
    workbook = setup

    num_bytes = 200 * 2**20
    num_fields_list = [3**x for x in range(5)]

    print('Truth,Units/Second,Dataset Size')

    for num_fields in num_fields_list:
        result = _run_benchmark_booleans(workbook, num_bytes, num_fields)
        duration = result['duration']
        size = result['size']
        print("{} fields,{},{}".format(num_fields, duration, size))


def _run_benchmark_ints(workbook, num_bytes, num_fields, field_range):
    single_row = {
        "field {}".format(ii): random.randint(field_range[0], field_range[1])
        for ii in range(num_fields)
    }
    row_size = len(single_row)
    num_rows = num_bytes // row_size
    data = [single_row for _ in range(num_rows)]
    return _run_benchmark(workbook, data)


def test_benchmark_ints(setup):
    workbook = setup

    random.seed(SEED)

    num_bytes = 200 * 2**20
    num_fields_list = [3**x for x in range(5)]
    range_vals = [[2**0, (2**8) - 1], [2**8, (2**16) - 1],
                  [2**16, (2**32) - 1], [2**32, (2**63) - 1]]
    range_keys = [
        "2**0 - 2**8", "2**8 - 2**16", "2**16 - 2**32", "2**32 - 2**63"
    ]
    durations_out = 'Units/sec,' + ','.join(
        ["{} ints".format(key) for key in range_keys]) + "\n"
    sizes_out = 'Dataset Sizes,' + ','.join(
        ["{} ints".format(key) for key in range_keys]) + "\n"

    for num_fields in num_fields_list:
        durations = []
        sizes = []
        for vals in range_vals:
            result = _run_benchmark_ints(workbook, num_bytes, num_fields, vals)
            durations.append(result['duration'])
            sizes.append(result['size'])
        field_durations = ','.join([str(d) for d in durations])
        durations_out = durations_out + "{} fields,{}\n".format(
            num_fields, field_durations)
        field_sizes = ','.join([str(s) for s in sizes])
        sizes_out = sizes_out + "{} fields, {}\n".format(
            num_fields, field_sizes)
    print('#' * 20 + ' DURATIONS ' + '#' * 20)
    print(durations_out)
    print('#' * 20 + ' DATASET SIZES ' + '#' * 20)
    print(sizes_out)


def _run_benchmark_datetimes(workbook, num_bytes, num_fields):
    single_row = {
        "field {}".format(ii): datetime.strptime('Feb 26 2010 1:59AM',
                                                 '%b %d %Y %I:%M%p')
        for ii in range(num_fields)
    }

    row_size = len(single_row)
    num_rows = num_bytes // row_size
    data = [single_row for _ in range(num_rows)]
    return _run_benchmark(workbook, data)


def test_benchmark_datetimes(setup):
    workbook = setup

    num_bytes = 200 * 2**20
    num_fields_list = [3**x for x in range(5)]

    print('Datetimes,Units/Second,Dataset Size')

    for num_fields in num_fields_list:
        result = _run_benchmark_datetimes(workbook, num_bytes, num_fields)
        duration = result['duration']
        size = result['size']
        print("{} fields,{},{}".format(num_fields, duration, size))
