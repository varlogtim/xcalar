import os
import json

# This generator processes the output of below and generates tests.
# aws s3 ls --recursive s3://xcfield/saas-load-test/Performance_Scalability_Test_Multiple/ > perfscal.out
# The test is very specific to above use case but quite handy.

basedir = "mytests"
SEED = 600
os.makedirs(basedir, exist_ok=True)
bucket = "saas-load-test"

def gen_tests(pffile, seeed, base_name):
    with open(pffile) as pf:
        entries = pf.readlines()
    dsdirs = []
    paths = []
    for entry in entries:
        paths.append(entry.split(" ")[-1].rstrip())

    seed = seeed
    base = base_name
    import_array = []
    param_array = []
    for sspath in paths:
        dsdir = os.path.dirname(sspath)
        if dsdir in dsdirs:
            continue

        test_case = {}
        test_case["name"] = f"{base}_{seed}"
        test_case["sample_file"] = None
        test_case["dataset"] = f"/{bucket}/{dsdir}/"
        test_case["test_type"] = base_name
        test_case["num_rows"] = 2
        test_case["recursive"] = False
        base_file = os.path.splitext(sspath)[0]

        if sspath.endswith(".csv"):

            # csv
            seed += 1
            csv_test_case = json.loads(json.dumps(test_case))
            csv_test_case["name"] = f"{base}_{seed}"
            csv_test_case["sample_file"] = f"/{bucket}/{base_file}.csv"
            csv_test_case["input_serialization"] = {"CSV" : {"FileHeaderInfo": "USE"}}
            csv_test_case["file_name_pattern"] = "*.csv"
            with open(f"{basedir}/testcase{seed}.py", "w") as tc:
                jj = json.dumps(csv_test_case, indent=4).replace("false", "False")
                tc.write(f"tc_{seed} = {jj}\n")
            param_array.append(f"        pytest.param(tc_{seed}, marks=pytest.mark.slow),")
            import_array.append(f"from test_cases.load.testcase{seed} import tc_{seed}")

            # gz
            file_name = f"{base_file}.csv.gz"
            if file_name in paths:
                seed += 1
                csvgz_test_case = json.loads(json.dumps(test_case))
                csvgz_test_case["name"] = f"{base}_{seed}"
                csvgz_test_case["sample_file"] = f"/{bucket}/{file_name}"
                csvgz_test_case["input_serialization"] = {"CompressionType" : "GZIP", "CSV" : {"FileHeaderInfo": "USE"}}
                csvgz_test_case["file_name_pattern"] = "*.gz"
                with open(f"{basedir}/testcase{seed}.py", "w") as tc:
                    jj = json.dumps(csvgz_test_case, indent=4).replace("false", "False")
                    tc.write(f"tc_{seed} = {jj}\n")
            param_array.append(f"        pytest.param(tc_{seed}, marks=pytest.mark.slow),")
            import_array.append(f"from test_cases.load.testcase{seed} import tc_{seed}")

            # bz2
            file_name = f"{base_file}.csv.bz2"
            if file_name in paths:
                seed += 1
                csvbz2_test_case = json.loads(json.dumps(test_case))
                csvbz2_test_case["name"] = f"{base}_{seed}"
                csvbz2_test_case["sample_file"] = f"/{bucket}/{file_name}"
                csvbz2_test_case["input_serialization"] = {"CompressionType" : "BZ2", "CSV" : {"FileHeaderInfo": "USE"}}
                csvbz2_test_case["file_name_pattern"] = "*.bz2"
                with open(f"{basedir}/testcase{seed}.py", "w") as tc:
                    jj = json.dumps(csvbz2_test_case, indent=4).replace("false", "False")
                    tc.write(f"tc_{seed} = {jj}\n")
            param_array.append(f"        pytest.param(tc_{seed}, marks=pytest.mark.slow),")
            import_array.append(f"from test_cases.load.testcase{seed} import tc_{seed}")

            dsdirs.append(dsdir)

        elif sspath.endswith(".json"):

            # json
            seed += 1
            json_test_case = json.loads(json.dumps(test_case))
            json_test_case["name"] = f"{base}_{seed}"
            json_test_case["sample_file"] = f"/{bucket}/{base_file}.json"
            json_test_case["input_serialization"] = {"JSON" : {"Type": "LINES"}}
            json_test_case["file_name_pattern"] = "*.json"
            with open(f"{basedir}/testcase{seed}.py", "w") as tc:
                jj = json.dumps(json_test_case, indent=4).replace("false", "False")
                tc.write(f"tc_{seed} = {jj}\n")
            param_array.append(f"        pytest.param(tc_{seed}, marks=pytest.mark.slow),")
            import_array.append(f"from test_cases.load.testcase{seed} import tc_{seed}")

            # gz
            file_name = f"{base_file}.json.gz"
            if file_name in paths:
                seed += 1
                jsongz_test_case = json.loads(json.dumps(test_case))
                jsongz_test_case["name"] = f"{base}_{seed}"
                jsongz_test_case["sample_file"] = f"/{bucket}/{file_name}"
                jsongz_test_case["input_serialization"] = {"CompressionType" : "GZIP", "JSON" : {"Type": "LINES"}}
                jsongz_test_case["file_name_pattern"] = "*.gz"
                with open(f"{basedir}/testcase{seed}.py", "w") as tc:
                    jj = json.dumps(jsongz_test_case, indent=4).replace("false", "False")
                    tc.write(f"tc_{seed} = {jj}\n")
            param_array.append(f"        pytest.param(tc_{seed}, marks=pytest.mark.slow),")
            import_array.append(f"from test_cases.load.testcase{seed} import tc_{seed}")

            # bz2
            file_name = f"{base_file}.json.bz2"
            if file_name in paths:
                seed += 1
                jsonbz2_test_case = json.loads(json.dumps(test_case))
                jsonbz2_test_case["name"] = f"{base}_{seed}"
                jsonbz2_test_case["sample_file"] = f"/{bucket}/{file_name}"
                jsonbz2_test_case["input_serialization"] = {"CompressionType" : "BZ2", "JSON" : {"Type": "LINES"}}
                jsonbz2_test_case["file_name_pattern"] = "*.bz2"
                with open(f"{basedir}/testcase{seed}.py", "w") as tc:
                    jj = json.dumps(jsonbz2_test_case, indent=4).replace("false", "False")
                    tc.write(f"tc_{seed} = {jj}\n")
            param_array.append(f"        pytest.param(tc_{seed}, marks=pytest.mark.slow),")
            import_array.append(f"from test_cases.load.testcase{seed} import tc_{seed}")

            dsdirs.append(dsdir)

        elif sspath.endswith(".parquet"):

            # parquet
            seed += 1
            parquet_test_case = json.loads(json.dumps(test_case))
            parquet_test_case["name"] = f"{base}_{seed}"
            parquet_test_case["sample_file"] = f"/{bucket}/{base_file}.parquet"
            parquet_test_case["input_serialization"] = {"Parquet" : {}}
            parquet_test_case["file_name_pattern"] = "*.parquet"
            with open(f"{basedir}/testcase{seed}.py", "w") as tc:
                jj = json.dumps(parquet_test_case, indent=4).replace("false", "False")
                tc.write(f"tc_{seed} = {jj}\n")
            param_array.append(f"        pytest.param(tc_{seed}, marks=pytest.mark.slow),")
            import_array.append(f"from test_cases.load.testcase{seed} import tc_{seed}")

            dsdirs.append(dsdir)
    with open("import_array.py", "w") as ia:
        ia.write('\n'.join(import_array))
    with open("param_array.py", "w") as ia:
        ia.write('\n'.join(param_array))


seed = SEED
base_name = "perf"
gen_tests("perfscal.out", seed, base_name)
