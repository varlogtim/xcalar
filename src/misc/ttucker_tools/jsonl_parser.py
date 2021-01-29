import re
import sys
import json
import pprint
from copy import deepcopy

USAGE = """
Purpose:
 - Parse JSONL
 - Uses first record as schema (expand later)
 - Detects if remaining records vary in schema.
 - ^ actually, let's just detect non-scalar types...

Exampe:
    {script} /path/to/file.jsonl

""".format(script=sys.argv[0])

if len(sys.argv) != 2:
    sys.stderr.write(USAGE)
    sys.stderr.flush()
    sys.exit(1)

file_path = sys.argv[1]

new_line_regex = re.compile(r'([^\n]+)')


def pp(o):
    pprint.pprint(o, indent=4)


def line_iter(s):
    for mm in re.finditer(new_line_regex, s):
        yield mm.group(0)


def schema_finder(obj, cur_path=[], mappings=[]):
    if not isinstance(obj, dict):
        raise ValueError("not dict")

    for key, val in obj.items():
        if isinstance(val, dict):
            new_path = [*cur_path, f'{key}']
            schema_finder(val, new_path, mappings)
        else:
            if val is not None:
                col_type = type(val)
            else:
                col_type = type("stringy")
            mapping = [*deepcopy(cur_path), key]
            out_obj = {"name": key,
                       "mapping": mapping,
                       "type": col_type}
            mappings.append(out_obj)
    if cur_path:
        cur_path.pop()
    return mappings


def get_dict(obj, path_array):
    # print(f"DERP: get_dict({obj}, {path_array})")
    if not path_array:
        # print(f"Final REturn from get_dict({obj})")
        return obj
    if obj is None:
        return obj
    first = path_array[0]
    rest = path_array[1:]
    return get_dict(obj.get(first), rest)


with open(file_path, 'r') as f:

    # This seems like it doesn't stream... hrmm...
    schema = []
    for line_num, line in enumerate(line_iter(f.read())):
        line_obj = json.loads(line)
        if line_num == 0:
            schema = schema_finder(line_obj, [], [])
            schema = [col for col in schema
                      if col["type"] is not list]
            continue
        # print(f"Schema({len(schema)}):")
        # pp(schema)
        # if line_num > 2:
        #     break
        for column in schema:
            name = column["name"]
            col_type = column["type"]
            mapping = column["mapping"]
            data = get_dict(line_obj, mapping)
            # if type(data) != col_type:
            #     print(f"{line_num}: Type Mismatch: {type(data)} != {col_type}. "
            #           f"NAME: {name}, DATA: {data}, MAPPING: {mapping}")
            if data and type(data) != col_type:
                print(f"{line_num}: Type Mismatch: {type(data)} != {col_type}. "
                      f"NAME: {name}, DATA: {data}, MAPPING: {mapping}")

