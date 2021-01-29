import io
import json

from ..connectors.util import File
import xcalar.container.target.base as target
from xcalar.container.cluster import get_running_cluster


@target.register("Snowflake Connector Target")


@target.param("role", "Name of role to use", optional=True)
@target.param("warehouse", "Name of warehouse to use", optional=True)
@target.param(
    "psw_provider",
    "Password provider module and function names. Default is plaintext",
    optional=True)
@target.param(
    "psw_arguments",
    "Arguments list for the password provider function",
    secret=True)
@target.param("schema", "Schema to use")
@target.param("dbname", "Name of the database to access")
@target.param("username", "User ID of the user for database server authentication")
@target.param("host", "Hostname or IP address of the database server")
@target.param("alias", "Identifier for this connector")
class SnowflakeTarget(target.BaseTarget):
    """
    Configures a connection to Snowflake.
    """

    def __init__(self,
                 name,
                 path,
                 host,
                 dbname,
                 username,
                 schema,
                 alias,
                 role=None,
                 warehouse=None,
                 psw_provider="plaintext",
                 psw_arguments=None,
                 **kwargs):
        super(SnowflakeTarget, self).__init__(name)
        self._config = {
            "host": host,
            "schema": schema,
            "dbname": dbname,
            "username": username,
            "alias": alias,
            "psw_provider": psw_provider,
            "psw_arguments": psw_arguments,
            "role": role,
            "warehouse": warehouse
        }

    def is_global(self):
        return True

    def get_files(self, path, name_pattern, recursive, **user_args):
        cluster = get_running_cluster()
        num_nodes = cluster.num_nodes
        xpus_per_node = cluster.xpus_per_node
        num_xpus = sum(xpus_per_node)
        return [
            File(path="/", relPath=self.name(), isDir=False, size=0, mtime=0),
        ] * num_xpus

    def open(self, path, opts):
        return io.BytesIO(json.dumps(self._config).encode("utf-8"))
