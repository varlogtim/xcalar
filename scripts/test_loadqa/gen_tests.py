import pytest
import json

from xc_load_test_cases import tests
from qa_test_gen import SEED

MARK = "mark"
BUCKET = "xcfield"

def dumpcase(testcases):
    tdump = json.dumps(testcases, indent=2)
    tdump = tdump.replace(": false", ": False")
    tdump = tdump.replace(": true", ": True")
    tdump = tdump.replace("\"pytest.mark.precheckin\"", "pytest.mark.precheckin")
    tdump = tdump.replace("\"pytest.mark.slow\"", "pytest.mark.slow")
    tdump = tdump.replace("\"pytest.mark.skip\"", "pytest.mark.skip")
    tdump = tdump.replace("\"pytest.mark.xfail\"", "pytest.mark.xfail")
    tdump = tdump.replace(": null", ": None")
    tdump = f"import pytest\n\ntestlist = {tdump}"
    return tdump


def get_testcases(tests):
    testcases = []
    perftestcases = []
    
    for test in tests:
        # Accessing ParameterSet object test below
        testcase = test[0][0]
        if len(test[1]) > 0:
            testcase["mark"] = f"pytest.mark.{test[1][0].name}"
        if testcase["sample_file"] is not None:
            testcase["sample_file"] = testcase["sample_file"].replace(f"{BUCKET}/", "")
        if testcase["dataset"] is not None:
            testcase["dataset"] = testcase["dataset"].replace(f"{BUCKET}/", "")
        testno = int(testcase["name"].split("_")[1])
        if testno < SEED+1:
            testcases.append(testcase) 
        else:
            perftestcases.append(testcase) 

    return dumpcase(testcases), dumpcase(perftestcases)



testcases, perftestcases = get_testcases(tests)
with open("manual_test_cases.py", "w") as mt:
    mt.write(f"{testcases}\n")
with open("auto_test_cases.py", "w") as mt:
    mt.write(f"{perftestcases}\n")
