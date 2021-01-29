from xcalar.external.client import Client
from xcalar.external.session import Session
from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException
from xcalar.external.LegacyApi.WorkItem import WorkItemDeleteDagNode
from xcalar.compute.coretypes.SourceTypeEnum.ttypes import SourceTypeT
from xcalar.compute.coretypes.Status.ttypes import StatusT
from structlog import get_logger

import json
from urllib.parse import urlunparse
logger = get_logger(__name__)


def get_client(remote_url: str, remote_username: str,
               remote_password: str) -> Client:
    """Gets the client object to interact with Xcalar

    Arguments:
        remote_url {str} -- [description]
        remote_username {str} -- [description]
        remote_password {str} -- [description]

    Returns:
        Client -- Xcalar Client Object
    """
    return Client(
        url=remote_url,
        client_secrets={
            'xiusername': remote_username,
            'xipassword': remote_password
        },
        bypass_proxy=False,
        user_name=remote_username)


def clone_client(client):
    if client.auth._using_proxy is False:
        return Client(bypass_proxy=True, user_name=client.username)
    else:
        client_secrets = json.loads(client.auth._client_secrets)
        url = urlunparse(client.auth.parsed_url)
        return get_client(
            remote_url=url,
            remote_username=client_secrets['xiusername'],
            remote_password=client_secrets['xipassword'])


def get_xcalarapi(remote_url: str, remote_username: str,
                  remote_password: str) -> XcalarApi:
    return XcalarApi(
        url=remote_url,
        client_secrets={
            'xiusername': remote_username,
            'xipassword': remote_password
        },
        bypass_proxy=False)


def destroy_or_create_session(client: Client, session: str) -> Session:
    """Destroys and recreates the session if already active, otherwise creates it

    Arguments:
        client {The Xcalar client object} -- The Client object.
        session {String} -- The name of the session.
    """
    try:
        sess = client.get_session(session)
        sess.destroy()
        return client.create_session(session)
    except Exception:
        return client.create_session(session)


def get_or_create_session(client: Client, session: str) -> Session:
    """Gets the session if already active otherwise creates it

    Arguments:
        client {The Xcalar client object} -- The Client object.
        session {String} -- The name of the session.
    """
    try:
        return client.get_session(session)
    except Exception as ex:
        logger.debug(f'***: {str(ex)}')
        return client.create_session(session)


def drop_tables(session: Session, name_pattern: str, delete_completely=True):
    workItem = WorkItemDeleteDagNode(
        name_pattern,
        SourceTypeT.SrcTable,
        userName=session.username,
        userIdUnique=session._user_id,
        deleteCompletely=delete_completely)
    try:
        session._execute(workItem)
    except XcalarApiStatusException as ex:
        if ex.status != StatusT.StatusTablePinned:
            # raise ex
            # TODO: need to follow up: why is backend trying to delete a table which is already locked?
            logger.debug(f'***: Error bulk dropping {str(ex)}')
