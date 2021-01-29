import os
import logging

from xcalar.container.connectors.mapr import MapRConnector, MAPR_TICKET_ENV_VAR
import xcalar.container.target.base as target

logger = logging.getLogger("xcalar")


@target.register(
    name="Secured MapR Cluster Account Impersonation",
    is_available=MapRConnector.is_available())
@target.param("service_ticket",
              "Path to the service ticket (ticket must be present on all "
              "Xcalar nodes and must have impersonation access)")
@target.param(
    "cldb_node",
    "CLDB node to connect to. This should match the service ticket "
    "and should be a CLDB node listed in "
    "MAPR_HOME/conf/mapr-clusters.conf (MAPR_HOME defaults to "
    "/opt/mapr). This defaults to a CLDB node from the "
    "first specified cluster.",
    optional=True)
@target.param(
    "cldb_port",
    "Port to use when connecting to cluster CLDB node",
    optional=True)
class MapRImpersonationTarget(target.BaseTarget):
    """
    Connect to MapR as the current Xcalar user.
    """

    def __init__(self,
                 name,
                 path,
                 service_ticket,
                 cldb_port=None,
                 cldb_node=None,
                 **kwargs):
        super(MapRImpersonationTarget, self).__init__(name)
        kw_args = {}
        self._service_ticket = service_ticket

        if cldb_port is not None:
            kw_args["port"] = cldb_port

        kw_args["user"] = kwargs.get('user_name', None)
        if kw_args["user"] is None:
            raise ValueError("user_name must be provided as input")

        if cldb_node is not None:
            kw_args["cldb_node"] = cldb_node

        os.environ[MAPR_TICKET_ENV_VAR] = self._service_ticket
        self.connector = MapRConnector(**kw_args)

    def is_global(self):
        return True

    def get_files(self, path, name_pattern, recursive, **user_args):
        return self.connector.get_files(path, name_pattern, recursive)

    def open(self, path, opts):
        return self.connector.open(path, opts)
