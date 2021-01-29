from xcalar.external.client import Client
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.session import Session


def get_client(remote_url: str,
               remote_username: str,
               remote_password: str,
               bypass_proxy: bool = False) -> Client:
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
            "xiusername": remote_username,
            "xipassword": remote_password
        },
        bypass_proxy=bypass_proxy,
        user_name=remote_username)


def get_xcalarapi(remote_url: str,
                  remote_username: str,
                  remote_password: str,
                  bypass_proxy: bool = False) -> XcalarApi:
    return XcalarApi(
        url=remote_url,
        client_secrets={
            "xiusername": remote_username,
            "xipassword": remote_password
        },
        bypass_proxy=bypass_proxy)


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
    except Exception:
        return client.create_session(session)
