from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.client import Client

testSessionSessionName = "TestSession"


def test_auth():
    xcalarApi = XcalarApi()
    xcalarApi.auth.login()
    cookies = xcalarApi.auth.cookies
    xcalarApi2 = XcalarApi(client_token=cookies['connect.sid'])
    xcalarApi2.auth.login()
    cookies2 = xcalarApi2.auth.cookies
    assert cookies['connect.sid'] != \
        cookies2['connect.sid']
    client = Client()
    workbook = client.create_workbook(testSessionSessionName)
    workbook.delete()
