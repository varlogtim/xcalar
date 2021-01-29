import pytest
from xcalar.external.exceptions import XDPException

from xcalar.compute.localtypes.Echo_pb2 import EchoRequest, EchoErrorRequest
from xcalar.external.services.echo_grpc import EchoClient

def testEcho():
    echoStr = "hello from the client!"
    request = EchoRequest(echo=echoStr)
    response = EchoClient().echoMessage(request)
    assert response.echoed == echoStr

def testErrors():
    errStr = "XCE-00000017 Invalid argument"
    request = EchoErrorRequest(error=errStr)
    with pytest.raises(XDPException) as e:
        EchoClient().echoErrorMessage(request)
    assert e.value.message == errStr

