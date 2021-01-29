import boto3
from botocore.client import Config
from botocore.handlers import disable_signing

from xcalar.container.connectors.s3 import S3Connector
from xcalar.container.connectors.object_store import ObjectStoreConnector
import xcalar.container.target.base as target


@target.register(
    name="S3 Account with Environmental Credentials",
    is_available=S3Connector.is_available())
@target.param(
    "authenticate_requests",
    "sign requests when connecting to S3 (default is true).",
    optional=True)
class S3EnvironTarget(target.BaseTarget):
    """
    Inherits the environmental setup for S3 using the standard boto3 scheme for
    obtaining authentication credentials. Set authenticate_requests to false if
    you only want to connect to public S3 buckets and do not need
    authentication.
    """

    def __init__(self, name, path, authenticate_requests=None, **kwargs):
        super(S3EnvironTarget, self).__init__(name)
        session = boto3.Session()
        config = Config(signature_version='s3v4')
        s3 = session.resource('s3', config=config)

        if (authenticate_requests is not None
                and authenticate_requests.lower() == "false"):
            # The user is accessing public S3 buckets only, so we will disable
            # auth for this target.
            s3.meta.client.meta.events.register('choose-signer.s3.*',
                                                disable_signing)

        s3_connector = S3Connector(s3)
        self.obj_store = ObjectStoreConnector(s3_connector)

    def is_global(self):
        return True

    # XXX Go through and rename all the targets to "list_files" because
    # "get_files" doesn't make a lot of sense.
    def get_files(self, path, name_pattern, recursive, **user_args):
        return self.obj_store.get_files(path, name_pattern, recursive)

    def get_files_paged(self, path, name_pattern, recursive, token=None):
        return self.obj_store.get_files_paged(path, name_pattern, recursive,
                                              token)

    def open(self, path, opts):
        return self.obj_store.open(path, opts)

    def delete(self, path):
        return self.obj_store.delete(path)
