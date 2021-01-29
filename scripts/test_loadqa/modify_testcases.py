import os
import json

tcpath = "../../src/bin/util/xcalar/compute/util/load_tests/test_cases/load/"
new_tcpath = "../../src/bin/util/xcalar/compute/util/load_tests/test_cases/load_new"
os.makedirs(new_tcpath, exist_ok=True)
testfiles = os.listdir(tcpath)

for testfile in testfiles:
    print(testfile)
    with open(os.path.join(tcpath, testfile)) as tf:
        tc = tf.read()
        tc = tc.replace("/saas-load-test/", "/xcfield/saas-load-test/")
        newname = f"{new_tcpath}/{testfile}"
        with open(newname, "w") as ntf:
            ntf.write(tc)
