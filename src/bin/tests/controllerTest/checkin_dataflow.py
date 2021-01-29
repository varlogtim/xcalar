import os
import sys
from pathlib import Path

from xcalar.solutions.git.git_api import GitApi
from xcalar.solutions.uber_dispatcher import UberDispatcher
from xcalar.solutions.xcalar_client_utils import (destroy_or_create_session,
                                                  get_client, get_xcalarapi)

sys.path.append(os.path.dirname(Path(__file__).parent))

REMOTE_URL = 'https://xdp-jdev-69.westus2.cloudapp.azure.com'
REMOTE_USERNAME = 'xdpadmin'
REMOTE_PASSWORD = 'Welcome1'
SESSION_NAME = 'test_integration_1'
os.environ['XLR_PYSDK_VERIFY_SSL_CERT'] = 'false'

if __name__ == "__main__":
    client = get_client(REMOTE_URL, REMOTE_USERNAME, REMOTE_PASSWORD)
    xcalarApi = get_xcalarapi(REMOTE_URL, REMOTE_USERNAME, REMOTE_PASSWORD)
    session = destroy_or_create_session(client, SESSION_NAME)
    xcalarApi.setSession(session)

    gitApi = GitApi('https://gitlab.com/api/v4', '14164315',
                    'Tavay5JsBjYzTq25DyXV')
    dispatcher = UberDispatcher(
        client,
        session=session,
        dsetSeed='tmp',
        xcalarApi=xcalarApi,
        vcsAdapter=gitApi,
        isDebug=True)

    dispatcher.checkin_dataflows(['refiner2'],
                                 'test',
                                 '/nikita/refiner_demo',
                                 workbook_name='kafka_dispatcher_nikita')
    print('Done')
