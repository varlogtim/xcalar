import os
import tempfile
import pytest
import shutil

from xcalar.compute.util.Qa import XcalarQaDatasetPath
from xcalar.xc2.fixworkbook import FixWorkbook

path_missing_udf = "/workbookUploadTests/missingMapUdfFunction.tar.gz"
path_missing_udf_fixed = "/workbookUploadTests/missingMapUdfFunction_fixed.tar.gz"
path_missing_udf_in_retina = "/workbookUploadTests/missingMapUdfFileInRetina.tar.gz"
path_missing_udf_in_retina_fixed = "/workbookUploadTests/missingMapUdfFileInRetina_fixed.tar.gz"
path_checksum_file_not_found = "/workbookUploadTests/checksumFileNotFound.tar.gz"
path_checksum_file_not_found_fixed = "/workbookUploadTests/checksumFileNotFound_fixed.tar.gz"
path_checksum_record_missing = "/workbookUploadTests/checksumRecordMissing.tar.gz"
path_checksum_record_missing_fixed = "/workbookUploadTests/checksumRecordMissing_fixed.tar.gz"
path_checksum_mismatch = "/workbookUploadTests/checksumMismatch.tar.gz"
path_checksum_mismatch_fixed = "/workbookUploadTests/checksumMismatch_fixed.tar.gz"
path_checksum_mismatch_v3 = "/workbookUploadTests/checksumMismatchV3.tar.gz"
path_checksum_mismatch_v3_fixed = "/workbookUploadTests/checksumMismatchV3_fixed.tar.gz"
path_checksum_mismatch_v4 = "/workbookUploadTests/checksumMismatchV4.tar.gz"
path_checksum_mismatch_v4_fixed = "/workbookUploadTests/checksumMismatchV4_fixed.tar.gz"
upload_name = "TestFixWorkbookSessionOwner" + "-upload"


@pytest.mark.parametrize(
    "missing_udf_file, missing_udf, path_missing_udf_workbook, path_missing_udf_workbook_fixed",
    [
        ("add1module.py", "def addone(field):\n    return str(int(field)+1)",
         path_missing_udf, path_missing_udf_fixed),
        ("m1.py", "def f1(a):\n    return a", path_missing_udf_in_retina,
         path_missing_udf_in_retina_fixed),
    ])
def test_fix_udf(client, missing_udf_file, missing_udf,
                 path_missing_udf_workbook, path_missing_udf_workbook_fixed):
    path_workbook = XcalarQaDatasetPath + path_missing_udf_workbook
    with open(path_workbook, "rb") as workbook_file:
        with pytest.raises(Exception) as e:
            uploaded_session = client.upload_workbook(upload_name,
                                                      workbook_file.read())
        assert e.value.message == "Could not find function"

    path_workbook_fixed = XcalarQaDatasetPath + path_missing_udf_workbook_fixed
    with tempfile.NamedTemporaryFile(mode="wt", encoding="utf-8") as udf_file:
        udf_file.write(missing_udf)
        udf_file.flush()
        shutil.copy(udf_file.name, missing_udf_file)
        fix_tool = FixWorkbook(
            local_path=[path_workbook],
            udf_path=missing_udf_file,
            output_path=os.path.dirname(path_workbook_fixed),
            recursive=True,
            silent=True)
        fix_tool.fix_workbook()
    os.remove(missing_udf_file)

    with open(path_workbook_fixed, "rb") as workbook_file:
        uploaded_session = client.upload_workbook(upload_name,
                                                  workbook_file.read())

    os.remove(path_workbook_fixed)
    uploaded_session.inactivate()
    uploaded_session.delete()


@pytest.mark.parametrize(
    "path_bad_workbook, path_bad_workbook_fixed, expected_error", [
        (path_checksum_file_not_found, path_checksum_file_not_found_fixed,
         "Checksum not found"),
        (path_checksum_record_missing, path_checksum_record_missing_fixed,
         "Checksum not found"),
        (path_checksum_mismatch, path_checksum_mismatch_fixed,
         "Checksum mismatch"),
        (path_checksum_mismatch_v3, path_checksum_mismatch_v3_fixed,
         "Checksum mismatch"),
        (path_checksum_mismatch_v4, path_checksum_mismatch_v4_fixed,
         "Checksum mismatch"),
    ])
def test_checksum(client, path_bad_workbook, path_bad_workbook_fixed,
                  expected_error):
    path_bad_workbook = XcalarQaDatasetPath + path_bad_workbook
    path_bad_workbook_fixed = XcalarQaDatasetPath + path_bad_workbook_fixed

    with open(path_bad_workbook, "rb") as bad_workbook:
        with pytest.raises(Exception) as e:
            uploaded_session = client.upload_workbook(upload_name,
                                                      bad_workbook.read())
        assert e.value.message == expected_error

    fix_tool = FixWorkbook(
        local_path=[path_bad_workbook],
        output_path=os.path.dirname(path_bad_workbook_fixed))
    fix_tool.fix_workbook()

    with open(path_bad_workbook_fixed, "rb") as fixed_workbook:
        uploaded_session = client.upload_workbook(upload_name,
                                                  fixed_workbook.read())

    os.remove(path_bad_workbook_fixed)
    uploaded_session.inactivate()
    uploaded_session.delete()
