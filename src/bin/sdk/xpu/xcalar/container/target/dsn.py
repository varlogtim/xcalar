import io
import json

from ..connectors.util import File
import xcalar.container.target.base as target


@target.register("Database Connector Target")
@target.param(
    "psw_provider",
    "Password provider module and function names",
    optional=True)
@target.param(
    "psw_arguments",
    "Arguments list for the password provider function",
    secret=True,
    optional=True)
@target.param(
    "auth_mode",
    "Hive Authentication Mode string. See Apache Hive documentation for values.",
    optional=True)
@target.param("uid", "User ID of the user for database server authentication")
@target.param("port", "Port where the database server accepts connections")
@target.param("host", "Hostname or IP address of the database server")
@target.param("dbname", "Name of the database to access")
@target.param("dbtype", "Type of the database - MSSQL/MySQL/Oracle/PG/Hive")
class DSNTarget(target.BaseTarget):
    """
    Configures a connection to SQL Databases, including MSSQL Server, MySQL, Oracle, PostgreSQL and Hive.
    """

    def __init__(self,
                 name,
                 path,
                 dbtype,
                 host,
                 port,
                 dbname,
                 uid,
                 psw_provider=None,
                 psw_arguments=None,
                 auth_mode=None,
                 **kwargs):
        super(DSNTarget, self).__init__(name)
        self._config = {
            "dbtype": dbtype,
            "host": host,
            "port": port,
            "dbname": dbname,
            "uid": uid,
            "psw_provider": psw_provider,
            "psw_arguments": psw_arguments,
            "auth_mode": auth_mode,
        }

    def is_global(self):
        return True

    def get_files(self, path, name_pattern, recursive, **user_args):
        return [
            File(path="/", relPath=self.name(), isDir=False, size=0, mtime=0)
        ]

    def open(self, path, opts):
        return io.BytesIO(json.dumps(self._config).encode("utf-8"))
