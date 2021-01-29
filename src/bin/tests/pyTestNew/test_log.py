import pytest
from xcalar.external.exceptions import XDPException
from xcalar.external.client import Client
from xcalar.external.LogLevel import XcalarSyslogMsgLevel
from xcalar.compute.localtypes.log_pb2 import SetLevelRequest

def testGetSetLevel():
    client = Client()
    # save the original value for restoration later
    org = client.get_log_level()

    # set log level to a new value
    log_level = XcalarSyslogMsgLevel.XlogNote \
        if org.log_level != XcalarSyslogMsgLevel.XlogNote \
        else XcalarSyslogMsgLevel.XlogInfo
    client.set_log_level(log_level)

    # verify log level is set to the new value
    response = client.get_log_level()
    assert response.log_level == log_level

    # restore log level back to original value
    log_level = org.log_level
    client.set_log_level(log_level)

    # confirm the log level is the original value
    response = client.get_log_level()
    assert response.log_level == org.log_level
