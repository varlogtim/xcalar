import pytest

import xcalar.external.client
from xcalar.external.exceptions import XDPException

from xcalar.compute.services.Echo_xcrpc import Echo, EchoRequest, EchoErrorRequest


def testEcho():
    echoStr = "hello from the client!"

    client = xcalar.external.client.Client()
    serv = Echo(client)
    echo = EchoRequest()
    echo.echo = echoStr
    resp = serv.echoMessage(echo)
    assert resp.echoed == echoStr


def testErrors():
    errStr = "XCE-00000017 Invalid argument"

    client = xcalar.external.client.Client()
    serv = Echo(client)
    err = EchoErrorRequest()
    err.error = errStr
    with pytest.raises(XDPException) as e:
        serv.echoErrorMessage(err)
    assert e.value.message == errStr
