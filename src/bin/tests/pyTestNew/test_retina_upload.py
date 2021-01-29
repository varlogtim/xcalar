# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

# Test infra to upgrade legacy retina files to DF2 dataflows.
#
# Here the name "retina" implies the pre-DF2 batch dataflow retina file.  So
# this test is about uploading the old retina files into DF2 dataflows (i.e.
# upgrading them and testing the upgraded DF2 dataflow - the upgrade success is
# evaluated by ensuring there's no failure on upload, and no failure on
# execution of the DF2 dataflow - unless of course the failure's expected).
#
# NOTE: retinas for source table tests are part of this but testing their
# execution post-upgrade is more work than is necessary - so the "uploadOnly"
# param is used to test only upload and not execution of these retinas. More
# detailed comments explain this later in this file
#
# NOTE: this test infra could be used to upload DF2 dataflows too but
# currently only legacy retina upgrades are being tested

import pytest
import random

from xcalar.compute.util.Qa import XcalarQaDatasetPath
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.Target2 import Target2
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.runtime import Runtime

memoryTargetName = "QA memory"
targets = [
    (memoryTargetName, "memory", {}),
]


@pytest.fixture(scope="module", autouse=True)
def setup():
    xcalarApi = XcalarApi()
    operators = Operators(xcalarApi)
    target = Target2(xcalarApi)

    for targetName, targetType, targetArgs in targets:
        target.add(targetType, targetName, targetArgs)
    yield


# This is mainly a legacy retina -> DF2 dataflow upgrade test. It could be used
# to upload DF2 dataflows but that's not the goal.
@pytest.mark.parametrize(
    "file_path, expected_error, execute_error,"
    "expectedNumRows, uploadOnly, exportToFile, exportToFileOnly",
    [
        ("/retinaTests/basic_ops_df.tar.gz", "", "", 999, "", "", ""),
        ("/retinaTests/retinaWithStreamingUDF.tar.gz", "", "", 1307, "", "",
         ""),
        ("/retinaTests/badDagState.tar.gz", "Error parsing JSON query", "", "",
         "", "", ""),
        ("/retinaTests/bug12514.tar.gz", "", "", 1, "", "", ""),
        ("/retinaTests/bug13684.tar.gz", "", "", 1, "", "", ""),
        ("/retinaTests/bug13831.tar.gz", "", "", 25, "", "", ""),
        ("/retinaTests/doubleIndex.tar.gz", "", "", 29, "", "", ""),
        ("/retinaTests/dupKeyIndex.tar.gz", "Invalid argument", "", 0, "", "",
         ""),
        ("/retinaTests/emptyTableBug.tar.gz", "", "", 141789, "", "", ""),
        ("/retinaTests/fatptrPrefixRename.tar.gz", "", "", 1, "", "", ""),
        ("/retinaTests/indexFilterPrefix.tar.gz", "", "", 934, "", "", ""),
        ("/retinaTests/indexFilter.tar.gz", "", "", 79, "", "", ""),
        ("/retinaTests/indexGroupBy.tar.gz", "", "", 4140, "", "", ""),
        ("/retinaTests/indexJoinProject.tar.gz", "", "", 1000000, "", "", ""),
        ("/retinaTests/indexProject.tar.gz", "", "", 1000, "", "", ""),
        ("/retinaTests/joinDropColumns.tar.gz", "", "", 1000, "", "", ""),
        ("/retinaTests/joinRename.tar.gz", "", "", 192, "", "", ""),
        ("/retinaTests/joinTest.tar.gz", "", "", 70815, "", "", ""),
        ("/retinaTests/nestedWithDelims.tar.gz", "", "", 1933, "", "", ""),
        ("/retinaTests/nestedWithDelims2.tar.gz", "", "", 991, "", "", ""),
        ("/retinaTests/nestedWithDelims3.tar.gz", "", "", 1000, "", "", ""),
        ("/retinaTests/projectImmediate.tar.gz", "", "", 1000, "", "", ""),
        ("/retinaTests/retinaBug11186FNFDescend.tar.gz", "", "", 5054, "", "",
         ""),
        ("/retinaTests/retinaBug11426FNF.tar.gz", "", "", 56, "", "", ""),
        ("/retinaTests/retinaBug12479.tar.gz", "", "", 100, "", "", ""),
    # Following test is the only one which sets exportToFile and
    # exportToFileOnly to True
        ("/retinaTests/retinaBug12493.tar.gz", "", "", 41602, False, True, True
         ),
        ("/retinaTests/retinaBug7491.tar.gz", "", "", 1423, "", "", ""),
        ("/retinaTests/retinaBug7700.tar.gz", "", "", 1184, "", "", ""),
        ("/retinaTests/retinaBug7862.tar.gz", "", "", 70817, "", "", ""),
        ("/retinaTests/retinaBug8673.tar.gz", "", "", 1000, "", "", ""),
        ("/retinaTests/retinaBug9222.tar.gz", "", "", 1000, "", "", ""),
        ("/retinaTests/retinaBug9765.tar.gz", "", "", 16306, "", "", ""),
        ("/retinaTests/rightJoin.tar.gz", "", "", 1000, "", "", ""),
        ("/retinaTests/selfJoinRename.tar.gz", "", "", 1000000, "", "", ""),
        ("/retinaTests/selfJoin.tar.gz", "", "", 1000, "", "", ""),
        ("/retinaTests/testExplode.tar.gz", "", "", 25590, "", "", ""),
        ("/retinaTests/testLargeExport.tar.gz", "", "", 100, "", "", ""),
        ("/retinaTests/test-multisort.tar.gz", "", "", 6, "", "", ""),
        ("/retinaTests/tpchq2.tar.gz", "", "", 100, "", "", ""),
        ("/retinaTests/customer12Batch.tar.gz", "", "", 5, "", "", ""),
        ("/retinaTests/unionDedup.tar.gz", "", "", 11, "", "", ""),

    # Following are retinas which test "with source tables" in pre-Dio
    # world (see doRetinatestWithSrc() in pyTest/test_retina.py) These
    # are included below - they're upload only (pass criteria is no
    # crash on upload; they will not execute since the old test used
    # query extraction to run the first retina in the chain, for
    # generating the conditions needed to execute the second in the
    # pair - porting this over is just over-engineering and unnecessary
    # to do chained retina execution - the latter is already covered as
    # part of the pyClient Dataflow runner work)
        ("/retinaTests/crossJoin.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/nestedWithDelims4Orig.tar.gz", "", "", "", True, "",
         ""),
        ("/retinaTests/nestedWithDelims4.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/nestedSynthesizeOrig.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/nestedSynthesize.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/retinaWithSrcProjectOrig.tar.gz", "", "", "", True, "",
         ""),
        ("/retinaTests/retinaWithSrcProject.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/retinaWithSrcLoadOrig.tar.gz", "", "", "", True, "",
         ""),
        ("/retinaTests/retinaWithSrcLoad.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/retinaWithEmptySrcOrig.tar.gz", "", "", "", True, "",
         ""),
        ("/retinaTests/retinaWithEmptySrc.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/unionFatptrDedupOrig.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/unionFatptrDedup.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/unionTestOrig.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/unionTest.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/mapTestOrig.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/mapTest.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/doubleOuterJoinOrig.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/doubleOuterJoin.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/bug10866Orig.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/bug10866.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/bug13481Orig.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/bug13481.tar.gz", "", "", "", True, "", ""),
        ("/retinaTests/edisonRetina-airlinesTest123.tar.gz", "", "", 138491,
         "", "", ""),
    ])
def test_retina_upload(client, file_path, expected_error, execute_error,
                       expectedNumRows, uploadOnly, exportToFile,
                       exportToFileOnly):
    retina_path = XcalarQaDatasetPath + file_path

    with open(retina_path, 'rb') as retFile:
        retContent = retFile.read()
        # create a per-test prefix using a common string and a string culled
        # from the file_path (the splits yield FOO from /X/FOO.tar.gz).
        # Following assumes file_path has the layout: /X/FOO.tar.gz
        testPrefix = "retinaUpgradeTests-" +    \
            file_path.split("/")[-1].split(".tar.gz")[0].split(".")[-1]

        datasetName = "{}-dataset".format(testPrefix)
        publishedDFName = "{}-publishedDF".format(testPrefix)

        if (expected_error):
            with pytest.raises(Exception) as e:
                uplRet = client.add_dataflow(publishedDFName, retContent)
                print("uploading '{}' failed with '{}'".format(
                    publishedDFName, e.message))
            assert e.value.message == expected_error
            return
        else:
            uplRet = client.add_dataflow(publishedDFName, retContent)
            print("uplRet is {}".format(uplRet))

        insideSessionName = publishedDFName + "-insideSession"
        inside_session = client.create_workbook(insideSessionName)
        inside_session.activate()

        if (uploadOnly):
            # only upload requested; skip execution
            client.delete_dataflow(publishedDFName)
            inside_session.inactivate()
            inside_session.delete()
            return

        if (execute_error):
            # NOTE: currently, this path isn't tested. i.e. no retinas in the
            # upgrade test bed expect to fail during execution
            exportTable = "myExportedTable"
            params = {
                'pathToQaDatasets': XcalarQaDatasetPath,
                'datasetName': datasetName,
                'tablePrefix': testPrefix
            }
            with pytest.raises(Exception) as e:
                clean_job = True
                parallel_ops = random.choice([True, False])
                sched_name = random.choice(Runtime.get_dataflow_scheds())

                ret = client.execute_dataflow(
                    publishedDFName,
                    inside_session,
                    params,
                    newTableName=exportTable,
                    parallel_operations=parallel_ops,
                    sched_name=sched_name,
                    clean_job_state=clean_job)
            assert e.value.message == execute_error
        else:
            exportTable = "myExportedTable"
            params = {
                'pathToQaDatasets': XcalarQaDatasetPath,
                'datasetName': datasetName,
                'tablePrefix': testPrefix
            }
            # If exportToFile is set, but not exportToFileOnly, then the
            # retina is executed twice - once to produce a table, and once to
            # export-to-file
            if (not exportToFileOnly):
                clean_job = True
                parallel_ops = random.choice([True, False])
                sched_name = random.choice(Runtime.get_dataflow_scheds())

                ret = client.execute_dataflow(
                    publishedDFName,
                    inside_session,
                    params,
                    newTableName=exportTable,
                    parallel_operations=parallel_ops,
                    sched_name=sched_name,
                    clean_job_state=clean_job)
            if (exportToFile):
                # NOTE: The files exported to <xcalarRoot>/export directory
                # aren't cleaned up. The test infra expects a brand new cluster
                # for each test run (e.g. an "xclean" between test runs). This
                # can be added if needed, but current runs don't require it
                clean_job = True
                parallel_ops = random.choice([True, False])
                sched_name = random.choice(Runtime.get_dataflow_scheds())

                ret = client.execute_dataflow(publishedDFName, inside_session,
                                              params,
                                              parallel_operations=parallel_ops,
                                              sched_name=sched_name,
                                              clean_job_state=clean_job)
                if (exportToFileOnly):
                    client.delete_dataflow(publishedDFName)
                    inside_session.inactivate()
                    inside_session.delete()
                    return

            if (expectedNumRows):
                xcalarApi = XcalarApi()
                operators = Operators(xcalarApi)
                xcalarApi.setSession(inside_session)
                output = xcalarApi.listTable(exportTable)
                assert (output.numNodes == 1)
                assert (output.nodeInfo[0].name == exportTable)
                output = operators.tableMeta(exportTable)
                retinaNumRows = sum([
                    output.metas[ii].numRows for ii in range(output.numMetas)
                ])
                assert (retinaNumRows == expectedNumRows)

        # clean up
        client.delete_dataflow(publishedDFName)
        inside_session.inactivate()
        inside_session.delete()
