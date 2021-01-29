import pytest
import os
import uuid
import subprocess
import shlex
import shutil
from xcalar.compute.util.config import detect_config

XLRDIR = os.environ["XLRDIR"]
XCE_CONFIG = detect_config().config_file_path
XcalarRootPath = detect_config().xcalar_root_path
try:
    XCE_LOGDIR = detect_config().all_options['Constants.XcalarLogCompletePath']
except KeyError as e:
    XCE_LOGDIR = '/var/log/xcalar'
os.chdir(XLRDIR)
xcalarVersion = subprocess.check_output(["git",
                                         "describe"]).strip().decode('utf-8')
UUID = uuid.uuid1()


def test_asup_generation():
    # As we run the tests on a single slave(3 nodes) hardcoding nodeId to 0, if not asups are generated thrice on the same machine.
    cmd = 'python3.6 {}/scripts/Support.py {} 0 {} {} {} {} "false" 0 0'.format(
        XLRDIR, UUID, XCE_CONFIG, XcalarRootPath, XCE_LOGDIR, xcalarVersion)
    args = shlex.split(cmd)
    output = subprocess.run(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    supportBackupDir = os.path.join(XcalarRootPath, 'support')
    supportBackupSupportId = os.path.join(supportBackupDir, str(UUID))
    if os.path.exists(supportBackupSupportId):
        shutil.rmtree(supportBackupSupportId)
    assert "Successfully generated support bundle" in output.stdout.decode(
        'utf-8') and output.returncode == 0
