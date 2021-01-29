# Copyright 2016-2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.


import string
import random
import json
import os
import pyarrow as pa
from pyarrow import parquet


def three_to_words(three):
    digits = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine']
    teens = ['ten', 'eleven', 'twelve', 'thirteen', 'fourteen', 'fifteen', 'sixteen', 'seventeen', 'eighteeen', 'nineteen']
    tens = ['ten', 'twenty', 'thirty', 'forty', 'fifty', 'sixty', 'seventy', 'eighty', 'ninety']
    hundred = 'hundred and'
    builder = []
    if len(three) == 3:
        if int(three[:1]) > 0:
            builder.append(f"{digits[int(three[:1])]} {hundred}")
    if len(three) >= 2:
        if int(three[-2:-1]) == 1:
            builder.append(f"{teens[int(three[-1:])]}")
        elif int(three[:2]) > 1:
            builder.append(f"{tens[int(three[-2:-1])-1]}")
            builder.append(f"{digits[int(three[-1:])].replace('zero', '')}")
    else:
        builder.append(f"{digits[int(three[-1:])].strip()}")
    return ' '.join(builder).strip()


def number_to_words(number):
    number = str(number)
    if len(number) > 15:
        raise ValueError("Number too large")
    part_size = 3
    powers = ['', 'thousand', 'million', 'billion', 'trillion']
    parts = [number[-1*(part_size)*(ii+1):(-1*part_size)*(ii)] if ii > 0 else number[(-1*part_size):] for ii in range(0, (len(number)//part_size)+1)]
    parts = parts[:1] if len(number) <= 3 else parts
    builder = []
    for jj,part in enumerate(parts):
        builder.append(f"{three_to_words(part)} {powers[jj]}")
    return ', '.join(list(reversed(builder))).strip()


def lettersum(fname):
    total=0
    for letter in fname:
        total += ord(letter)
    return total


def get_str_val(fname, row_ind, col_ind):
    ls = lettersum(fname)
    sum_of_digits = sum(int(digit) for digit in str(row_ind+col_ind)) + ls
    return number_to_words(lettersum(number_to_words(sum_of_digits)))


def get_int_val(fname, row_ind, col_ind):
    ls = lettersum(fname)
    return sum(int(digit) for digit in str(row_ind+col_ind)) + ls


def get_float_val(fname, row_ind, col_ind):
    ls = lettersum(fname)
    return row_ind/col_ind + ls


def get_bool_val(fname, row_ind, col_ind):
    ls = lettersum(fname)
    return row_ind/col_ind > 1


get_val = {
        str: get_str_val,
        int: get_int_val,
        float: get_float_val,
        bool: get_bool_val
        }


def get_csv_recs(fname, num_rows, num_cols):
    hdr = [number_to_words(digit).upper() for digit in range(num_cols)]
    header = '|'.join(hdr)

    rows = []
    rows.append(header)
    for row_ind in range(num_rows):
        row = []
        for col_ind in range(num_cols):
            if col_ind == 0:
                row.append(str(row_ind))
            elif col_ind == 1:
                row.append(fname)
            else:
                row.append(get_val[str](fname, row_ind, col_ind))
        rows.append('|'.join(row))

    return rows


def get_json_recs(fname, num_rows, num_cols):
    hdr = [number_to_words(digit).upper() for digit in range(num_cols)]

    rows = []
    for row_ind in range(num_rows):
        row = {}
        for col_ind in range(num_cols):
            if col_ind == 0:
                row[hdr[col_ind]] = str(row_ind)
            elif col_ind == 1:
                row[hdr[col_ind]] = fname
            else:
                row[hdr[col_ind]] = get_val[str](fname, row_ind, col_ind)
        rows.append(json.dumps(row))

    return rows


def get_parquet_str_recs(fname, num_rows, num_cols):
    hdr = [number_to_words(digit).upper() for digit in range(num_cols)]

    dfdata = [[] for col in range(len(hdr))]
    for col_ind in range(num_cols):
        for row_ind in range(num_rows):
            if col_ind == 0:
                dfdata[col_ind].append(str(row_ind))
            elif col_ind == 1:
                dfdata[col_ind].append(fname)
            else:
                dfdata[col_ind].append(get_val[str](fname, row_ind, col_ind))

    data_arrays = [pa.array(dfdata[cind]) for cind in range(num_cols)]
    fields = [pa.field(col, pa.string()) for col in hdr]
    table = pa.Table.from_arrays(data_arrays, schema=pa.schema(fields))

    return table


def get_parquet_int_recs(fname, num_rows, num_cols):
    hdr = [number_to_words(digit).upper() for digit in range(num_cols)]

    dfdata = [[] for col in range(len(hdr))]
    for col_ind in range(num_cols):
        for row_ind in range(num_rows):
            if col_ind == 0:
                dfdata[col_ind].append(str(row_ind))
            elif col_ind == 1:
                dfdata[col_ind].append(fname)
            else:
                dfdata[col_ind].append(get_val[int](fname, row_ind, col_ind))

    data_arrays = [pa.array(dfdata[cind]) for cind in range(num_cols)]
    fields = [pa.field(col, pa.int64()) if col_ind > 1 else pa.field(col, pa.string()) for col_ind, col in enumerate(hdr)]
    table = pa.Table.from_arrays(data_arrays, schema=pa.schema(fields))

    return table


def get_parquet_float_recs(fname, num_rows, num_cols):
    hdr = [number_to_words(digit).upper() for digit in range(num_cols)]

    dfdata = [[] for col in range(len(hdr))]
    for col_ind in range(num_cols):
        for row_ind in range(num_rows):
            if col_ind == 0:
                dfdata[col_ind].append(str(row_ind))
            elif col_ind == 1:
                dfdata[col_ind].append(fname)
            else:
                dfdata[col_ind].append(get_val[float](fname, row_ind, col_ind))

    data_arrays = [pa.array(dfdata[cind]) for cind in range(num_cols)]
    fields = [pa.field(col, pa.float64()) if col_ind > 1 else pa.field(col, pa.string()) for col_ind, col in enumerate(hdr)]
    table = pa.Table.from_arrays(data_arrays, schema=pa.schema(fields))

    return table


def get_parquet_bool_recs(fname, num_rows, num_cols):
    hdr = [number_to_words(digit).upper() for digit in range(num_cols)]

    dfdata = [[] for col in range(len(hdr))]
    for col_ind in range(num_cols):
        for row_ind in range(num_rows):
            if col_ind == 0:
                dfdata[col_ind].append(str(row_ind))
            elif col_ind == 1:
                dfdata[col_ind].append(fname)
            else:
                dfdata[col_ind].append(get_val[bool](fname, row_ind, col_ind))

    data_arrays = [pa.array(dfdata[cind]) for cind in range(num_cols)]
    fields = [pa.field(col, pa.bool_()) if col_ind > 1 else pa.field(col, pa.string()) for col_ind, col in enumerate(hdr)]
    table = pa.Table.from_arrays(data_arrays, schema=pa.schema(fields))

    return table


def create_csv_dataset(prefix, num_files, num_rows, num_cols):
    folder = f"datasets/csv/{num_files}_{num_rows}_{num_cols}"
    os.makedirs(folder, exist_ok=True)
    for ff in range(num_files):
        fname = f"{prefix}{ff}.csv"
        recs = get_csv_recs(fname, num_rows, num_cols)
        with open(os.path.join(folder, fname), "w") as intf:
            intf.write('\n'.join(recs) + "\n")


def create_json_dataset(prefix, num_files, num_rows, num_cols):
    folder = f"datasets/json/{num_files}_{num_rows}_{num_cols}"
    os.makedirs(folder, exist_ok=True)
    for ff in range(num_files):
        fname = f"{prefix}{ff}.json"
        recs = get_json_recs(fname, num_rows, num_cols)
        with open(os.path.join(folder, fname), "w") as intf:
            intf.write('\n'.join(recs) + "\n")

parquet_gen_map = {
            str : get_parquet_str_recs,
            int : get_parquet_int_recs,
            float : get_parquet_float_recs,
            bool : get_parquet_bool_recs
        }

def create_parquet_dataset(prefix, num_files, num_rows, num_cols, dtype):
    folder = f"datasets/parquet/{dtype}/{num_files}_{num_rows}_{num_cols}"
    os.makedirs(folder, exist_ok=True)
    for ff in range(num_files):
        fname = f"{prefix}{ff}.parquet"
        table = parquet_gen_map[dtype](fname, num_rows, num_cols)
        pa.parquet.write_table(table, os.path.join(folder, fname))


if __name__ == "__main__":
    num_cols = 10
    create_csv_dataset("file", num_files=2**5, num_rows=2**5, num_cols=num_cols)
    create_csv_dataset("file", num_files=2**10, num_rows=2**10, num_cols=num_cols)
    create_json_dataset("file", num_files=2**5, num_rows=2**5, num_cols=num_cols)
    create_json_dataset("file", num_files=2**10, num_rows=2**10, num_cols=num_cols)
    create_parquet_dataset("file", num_files=2**10, num_rows=2**10, num_cols=num_cols, dtype=str)
    create_parquet_dataset("file", num_files=2**5, num_rows=2**5, num_cols=num_cols, dtype=str)
    create_parquet_dataset("file", num_files=2**5, num_rows=2**5, num_cols=num_cols, dtype=int)
    create_parquet_dataset("file", num_files=2**5, num_rows=2**5, num_cols=num_cols, dtype=float)
    create_parquet_dataset("file", num_files=2**5, num_rows=2**5, num_cols=num_cols, dtype=bool)
