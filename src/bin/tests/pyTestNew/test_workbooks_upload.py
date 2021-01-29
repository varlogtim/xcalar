# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import pytest

from xcalar.compute.util.Qa import XcalarQaDatasetPath


@pytest.mark.parametrize(
    "file_path, expected_error, activate_error",
    [
    # Fails on upload as the workbook file contains a version higher than what
    # is currently supported.
        ("/workbookUploadTests/futureVersion.tar.gz",
         "Invalid workbook version", ""),
    # Fails on upload as the workbook file is missing the UDF module that is being used for a sUDF.
        ("/workbookUploadTests/missingLoadUdfFile.tar.gz",
         "No such file or directory", ""),
    # Fails on upload as the workbook is missing the UDF module that is being used for a Map.
        ("/workbookUploadTests/missingMapUdfFile.tar.gz",
         "No such file or directory", ""),
    # Fails on upload as the workbook is missing the UDF module that is being used for a Map inside the embedded batch dataflow.
        ("/workbookUploadTests/missingMapUdfFileInRetina.tar.gz",
         "Could not find function", ""),
    # Fails on upload as the UDF module exists but is missing the Map UDF function that is
    # being used.
        ("/workbookUploadTests/missingMapUdfFunction.tar.gz",
         "Could not find function", ""),
    # Fails on upload as the workbook file is missing the workbook header.
        ("/workbookUploadTests/missingWBHeader.tar.gz",
         "No such file or directory", ""),
    # Fails on upload as the workbook file is missing the KVS section.
        ("/workbookUploadTests/missingWBKvStore.tar.gz",
         "No such file or directory", ""),
    # Fails on upload as the workbook file is missing thue query graph.
        ("/workbookUploadTests/missingWBQGraph.tar.gz",
         "No such file or directory", ""),
    # Fails on upload as the workbook file is missing the session info.
        ("/workbookUploadTests/missingWBSession.tar.gz",
         "No such file or directory", ""),
    # Fails on upload as the workbook file is version 2 but doesn't have checksum file.
        ("/workbookUploadTests/checksumFileNotFound.tar.gz",
         "Checksum not found", ""),
    # Fails on upload as the workbook file is version 2 but checksum for one of the files is missing.
        ("/workbookUploadTests/checksumRecordMissing.tar.gz",
         "Checksum not found", ""),
    # Fails on upload as the workbook file is version 2 but there is a checksum mismatch.
        ("/workbookUploadTests/checksumMismatch.tar.gz", "Checksum mismatch",
         ""),
    # This workbook loads successfully
        ("/workbookUploadTests/validWorkbook.tar.gz", "", ""),
    # This workbook should load successfully (witness to Xc-12373)
        ("/workbookUploadTests/droppedDagNodes.tar.gz", "", ""),
    # This workbook should load successfully (witness to Xc-12422) and download
    # successfully. That's the main goal for this test, since the workbook is
    # very large and reflects a real customer use case with a complex qgraph and
    # several UDFs. This will exercise the large protobuf message limit during
    # download (which triggers deserialize), since it creates very large
    # protobuf messages (and will fail on the default 64MB protobuf limit, since
    # its size exceeds 64MB).
    #
    # NOTE: It'll NOT fail to activate even though it's a real workbook
    # from a customer (customer3) which references published tables
    # that don't exist in our test bed. This is because activation does
    # not trigger automatic replay of the query graph. In fact, this
    # and other older workbooks need to be upgraded to the new layout
    # in which the qgraph is moved to the KVS.
    # XXX: And this script could extract the upgraded query from the
    # workbook's KVS and execute it - and verify that it gets the
    # "Publish table name not found" error
        ("/workbookUploadTests/customer3BigWorkbook.tar.gz", "", ""),
    #
    # This workbook should fail on upload (witness to Xc-13097) without crashing
    # usrnode. The upload failure should be due to a missing UDF
    # (batchcontext:asOfCutOffDate_Price which has been purposefully misspelt in the
    # udf section to trigger a failure during upload which destroys the dag)
        ("/workbookUploadTests/GSDagCorruptionWorkbook.tar.gz",
         "Could not find function", ""),
    # This workbook should load successfully (witness to Xc-13840)
        ("/workbookUploadTests/droppedLoadNodes.tar.gz", "", ""),
    # This test is currently disabled as it hits an assert on DEBUG builds.
        pytest.param(
            "/workbookUploadTests/missingIndex.tar.gz",
            "Session already exists",
            "",
            marks=pytest.mark.skip(reason="hits assert on debug builds")),
    # This test should Fail when its qgraph is replayed as the UDF
    # module exists but is missing the specific sUDF function that is
    # being used. However, on activate, there's no replay now, and so
    # activation at least will succeed. This will fail when and if the
    # qgraph is replayed post-upload/upgrade/activate.
    #
    # XXX: this script could extract the query from the workbook's KVS
    # and execute it, and verify that it gets the
    # "Failed to execute user-defined function/application" error
        pytest.param("/workbookUploadTests/missingLoadUdfFunction.tar.gz", "",
                     ""),
    ])
def test_upload(client, file_path, expected_error, activate_error):
    workbook_path = XcalarQaDatasetPath + file_path

    testUploadSessionOwner = "TestUploadSessionOwner"
    with open(workbook_path, 'rb') as wbFile:
        wbContent = wbFile.read()
        uploadName = testUploadSessionOwner + "-upload"

        if (expected_error):
            with pytest.raises(Exception) as e:
                uplS = client.upload_workbook(uploadName, wbContent)
                print("uploading '{}' failed with '{}'".format(
                    uploadName, e.message))
            assert e.value.message == expected_error
            return
        else:
            uplS = client.upload_workbook(uploadName, wbContent)
            print("uplS is {}".format(uplS))

        if (activate_error):
            with pytest.raises(Exception) as e:
                uplS.activate()
            assert e.value.message == activate_error
        else:
            uplS.activate()

        # clean uo
        uplS.inactivate()
        uplS.delete()
