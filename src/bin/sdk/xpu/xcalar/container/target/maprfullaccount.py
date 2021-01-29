import logging

from xcalar.container.connectors.mapr import MapRConnector, MapRAuth
import xcalar.container.target.base as target

logger = logging.getLogger("xcalar")


@target.register(
    name="Secured MapR Cluster Account",
    is_available=MapRConnector.is_available())
@target.param(
    "cluster_name",
    "Cluster name @target.param(check /opt/mapr/conf/mapr-clusters.conf)")
@target.param(
    "cldb_port",
    "Port to use when connecting to cluster CLDB node)",
    optional=True)
@target.param("username", "Username")
@target.param("password", "password", secret=True)
class MapRFullAccountTarget(target.BaseTarget):
    """
    Manually specifies the credentials for a MapR account.
    """

    def __init__(self,
                 name,
                 path,
                 cluster_name,
                 username,
                 password,
                 cldb_port=None,
                 **kwargs):
        super(MapRFullAccountTarget, self).__init__(name)
        kw_args = {}
        cluster_name = cluster_name
        user = username
        password = password

        # Note that we do NOT add the user to the MapRConnector. We just let
        # the ticket carry the user information implicitly.
        if cldb_port is not None:
            kw_args["port"] = cldb_port

        self.auth = MapRAuth(user, password, cluster_name)
        self.connector = MapRConnector(**kw_args)

    def __enter__(self):
        self.auth.connect()
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        self.auth.disconnect()

    def is_global(self):
        return True

    def get_files(self, path, name_pattern, recursive, **user_args):
        return self.connector.get_files(path, name_pattern, recursive)

    def open(self, path, opts):
        return self.connector.open(path, opts)
