import pytest
from xcalar.external.exceptions import XDPException
from xcalar.external.client import Client

def testGetVersion():
    client = Client()
    response = client.get_version()
    print (response)
    assert response.version.startswith('xcalar-')
