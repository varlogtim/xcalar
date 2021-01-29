from xcalar.compute.services.Echo_xcrpc import Echo, EchoRequest

from xcalar.external.client import Client


def test_hello_world():
    client = Client()
    echoStr = "Hello World from client"
    serv = Echo(client)
    echo = EchoRequest()
    echo.echo = echoStr
    resp = serv.echoMessage(echo)

    assert resp.echoed == echoStr
