import pytest
from xcalar.compute.util.tests_util import upload_stats_test_util
from xcalar.solutions.tools.connector import XShell_Connector
from xcalar.compute.util.Qa import XcalarQaDatasetPath


@pytest.mark.parametrize(
    "file_path, dataflow_name",
    [
    # Simple dataflows in the same workbook - first two end with linkout
    # optimized
        ("/dataflowExecuteTests/simpleTests.tar.gz", "DF1"),
    ])
def test_upload_stats_command(shell, file_path, dataflow_name):
    workbook_path = XcalarQaDatasetPath + file_path
    client = XShell_Connector.get_client()
    xcalar_api = XShell_Connector.get_xcalarapi()
    upload_stats_test_util(client, xcalar_api, workbook_path, dataflow_name, shell)
