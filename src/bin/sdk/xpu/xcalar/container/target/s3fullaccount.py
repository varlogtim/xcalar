import logging

import boto3
from botocore.client import Config

from xcalar.container.connectors.s3 import S3Connector
from xcalar.container.connectors.object_store import ObjectStoreConnector
import xcalar.container.target.base as target

logger = logging.getLogger("xcalar")


@target.register(
    name="S3 Account with Specified Credentials",
    is_available=S3Connector.is_available())
@target.param("access_key", "S3 access key")
@target.param("secret_access_key", "S3 secret access key", secret=True)
@target.param(
    "endpoint_url", "URL to use when accessing the bucket", optional=True)
@target.param(
    "region_name",
    "Region to use when connecting to S3 (required if using 'verify')",
    optional=True)
@target.param(
    "verify",
    "Verify SSL certificate when connecting to S3 (default is true)",
    optional=True)
class S3FullAccountTarget(target.BaseTarget):
    """
    Specify the access credentials for your AWS S3 bucket account.
    """

    def __init__(self,
                 name,
                 path,
                 access_key,
                 secret_access_key,
                 endpoint_url=None,
                 region_name=None,
                 verify=None,
                 **kwargs):
        super(S3FullAccountTarget, self).__init__(name)
        session_opts = {}
        session_opts["aws_access_key_id"] = access_key
        session_opts["aws_secret_access_key"] = secret_access_key

        if region_name:
            session_opts["region_name"] = region_name

        session = boto3.Session(**session_opts)

        resource_opts = {}
        if endpoint_url:
            resource_opts["endpoint_url"] = endpoint_url
        if verify is not None:
            resource_opts["verify"] = verify.lower() != "false"

        resource_opts["config"] = Config(signature_version='s3v4')
        s3 = session.resource('s3', **resource_opts)

        s3_connector = S3Connector(s3)
        self.obj_store = ObjectStoreConnector(s3_connector)

    def is_global(self):
        return True

    def get_files(self, path, name_pattern, recursive, **user_args):
        return self.obj_store.get_files(path, name_pattern, recursive)

    def get_files_paged(self, path, name_pattern, recursive, token=None):
        return self.obj_store.get_files_paged(path, name_pattern, recursive,
                                              token)

    def open(self, path, opts):
        return self.obj_store.open(path, opts)

    def delete(self, path):
        return self.obj_store.delete(path)
