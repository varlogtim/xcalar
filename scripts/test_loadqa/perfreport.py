import json

from auto_test_cases import testlist as auto_tests
from manual_test_cases import testlist as manual_tests
from xc_load_types import DATASET, NAME

#perf_path = "/netstore/datasets/loadqa/perfout_my-el7-build.json"
perf_path = "/netstore/datasets/loadqa/perfout_ip-10-20-0-40.us-west-2.compute.internal.json"


def get_test(name):
    for testcase in auto_tests:
        if testcase[NAME] == name:
            return testcase
    return None

def update_report(perf_path):
    with open(perf_path, "r") as pp:
        jj = json.loads(pp.read())
    csvdata = []
    for test_id,test_name in enumerate(jj["tests"]):
        testcase = get_test(test_name)
        if testcase is None:
            continue
        if test_id == 0:
            header_keys = list(jj["tests"][test_name].keys())
        dataset = testcase[DATASET]
        dparts = dataset.split("/")
        file_type = dparts[3]
        num_files_str = dparts[4].split("_")[0]
        if num_files_str == "one":
            num_files = 1
        else:
            num_files = int(num_files_str)
        data_type = dparts[5]
        if data_type == "parse":
            data_type == "sparse-mix"
        num_rows_str,num_cols_str = dparts[6].split("_")
        if "10k" in num_rows_str:
            num_rows = 10000
        elif "100k" in num_rows_str:
            num_rows = 100000
        elif "1m" in num_rows_str:
            num_rows = 1000000
        elif "10m" in num_rows_str:
            num_rows = 10000000
        else:
            raise ValueError(f"unexpected num rows {num_rows_str}")
        if num_cols_str == "100col":
            num_cols = 100
        elif num_cols_str == "1000col":
            num_cols = 1000
        else:
            raise ValueError(f"unexpected num cols {num_cols_str}")

        jj["tests"][test_name]["file_type"] = file_type
        jj["tests"][test_name]["num_files"] = num_files
        jj["tests"][test_name]["data_type"] = data_type
        jj["tests"][test_name]["num_rows"] = num_rows
        jj["tests"][test_name]["num_cols"] = num_cols
        if test_id == 0:
            header_keys.append("file_type")
            header_keys.append("num_files")
            header_keys.append("data_type")
            header_keys.append("num_rows")
            header_keys.append("num_cols")
            csvdata.append(','.join(header_keys))
        else:
            row = []
            for col in header_keys:
                row.append(str(jj["tests"][test_name][col]))
            csvdata.append(','.join(row))
    with open("perfout_update.json", "w") as pu:
        pu.write(json.dumps(jj, indent=2) + "\n")
    with open("perfout_update_jsonl.json", "w") as pu:
        tests = []
        for ii in jj["tests"]:
            tests.append(json.dumps(jj["tests"][ii]))
        pu.write('\n'.join(tests) + "\n")
    with open("perfout_update.csv", "w") as pu:
        pu.write('\n'.join(csvdata) + "\n")


update_report(perf_path)
