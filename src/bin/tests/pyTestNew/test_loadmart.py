import pytest
import subprocess
import shlex
import datetime
import os
from contextlib import contextmanager
from xcalar.external.client import Client
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.Operators import Operators

table_record_dict = {
    'LM_FILE_TABLE': 3,
    'LM_PREVIEW_TABLE': 3,
    'LM_LOADED_TABLES': 9
}


# Generate logs for the loadmart to parse and load them into tables
# Helper Function
def generate_logs():
    now = datetime.datetime.now()
    starttime = (now.isoformat()).split('.')[0]
    # pytest that generates data
    tests = ['test_32', 'test_35', 'test_47']
    for test in tests:
        print("Running Testcase {}:".format(test))
        cmd = "python -m pytest -v --loadtestcase {} {}/scripts/test_loadqa/test_loadqa_suite.py -k specific".format(
            test, os.environ["XLRDIR"])
        args = shlex.split(cmd)
        resp = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = resp.communicate()
        if resp.returncode != 0:
            raise Exception("Generating logs failed: {}".format(err))
    now = datetime.datetime.now()
    endtime = (now.isoformat()).split('.')[0]
    return starttime, endtime


@contextmanager
def setup_loadmart():
    starttime, endtime = generate_logs()
    cmd = "python {}/scripts/loadmart_stats_uploader.py".format(
        os.environ["XLRDIR"])
    args = shlex.split(cmd)
    output = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = output.communicate()
    if output.returncode != 0:
        raise Exception("Uploading stats failed for loadmart: {}".format(err))
    client = Client()
    session = client.get_session("LoadMart")
    xcalarapi = XcalarApi()
    xcalarapi.setSession(session)
    operators = Operators(xcalarapi)
    try:
        yield (session, operators, starttime, endtime)
    finally:
        for table in table_record_dict.keys():
            try:
                operators.unpublish(table)
            except Exception:
                pass
        session.destroy()


# Loadmart basically loads everything that matches in the logs, so if other tests loads tables its hard to check the correctness.
# Hence I am executing 3 testcases which should generate 3,9,3 rows in LM_FILE_TABLE,LM_LOADED_TABLE,LM_PREVIEW_TABLE
# To verify correctness i need the timestamps of when these testcases ran and can query the published tables
@pytest.mark.skip("ENG-10525 load was changed, load mart will not work")
def test_loadmart():
    with setup_loadmart() as (sess, op, st, et):
        # table_record_dict is published_tablename: expected_record_count
        for published_tablename in table_record_dict.keys():
            list_out = op.listPublishedTables(published_tablename)
            assert len(list_out.tables) == 1
            sess.execute_sql(
                "SELECT * FROM {} WHERE TIMESTAMP BETWEEN '{}' AND '{}'".
                format(published_tablename, st,
                       et), "sql_{}".format(published_tablename))
            t = sess.get_table(f'sql_{published_tablename}')
            assert t.record_count() == table_record_dict[published_tablename]
            for row in range(table_record_dict[published_tablename]):
                record = t.get_row(row)
                for key in record.keys():
                    assert len(str(record[key])) != 0
