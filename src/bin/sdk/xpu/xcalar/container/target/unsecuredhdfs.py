import snakebite.client

from xcalar.container.connectors.snakebitehdfs import HDFSConnector
from base import TargetParam, BaseTarget

import logging
logger = logging.getLogger("xcalar")

name = "Unsecured HDFS"

description = """
Connects to an HDFS cluster using snakebite and no security protocols.
"""

params = [
    TargetParam("hostname", "HDFS NameNode hostname"),
    TargetParam("port", "HDFS NameNode port (default 8020)", optional=True),
]


def is_available():
    return HDFSConnector.is_available()


def get_name():
    return name


def list_parameters():
    return params


def get_description():
    return description.strip('\n').replace('\n', ' ')


def get_target(name, target_params, path):
    return HDFSUnsecuredTarget(name, target_params)


class HDFSUnsecuredTarget(BaseTarget):
    def __init__(self, name, target_params, **kwargs):
        super(HDFSUnsecuredTarget, self).__init__(name)
        port = 8020
        try:
            port = int(target_params["port"])
        except ValueError:
            # allow the port to be invalid; fall back to default
            pass
        hostname = target_params["hostname"]
        client = snakebite.client.Client(
            hostname, port, use_datanode_hostname=True)

        self.connector = HDFSConnector(client)

    def is_global(self):
        return True

    def get_files(self, path, name_pattern, recursive, **user_args):
        return self.connector.get_files(path, name_pattern, recursive)

    def open(self, path, opts):
        return self.connector.open(path, opts)
