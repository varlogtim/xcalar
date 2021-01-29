from xcalar.container.connectors.mock import MockConnector
from xcalar.container.connectors.object_store import ObjectStoreConnector
import xcalar.container.target.base as target

import logging

logger = logging.getLogger('xcalar')


@target.register(
    name="Mock Account with Environmental Credentials",
    is_available=MockConnector.is_available())
@target.param(
    "authenticate_requests",
    "sign requests when connecting to S3 (default is true).",
    optional=True)
class MockTarget(target.BaseTarget):
    """
    Create a Mock target hat does not hit a cloud service for any reason.
    """

    def __init__(self, name, path, authenticate_requests=None, **kwargs):
        super(MockTarget, self).__init__(name)

        mock_connector = MockConnector()
        self.obj_store = ObjectStoreConnector(mock_connector)

    def is_global(self):
        return True

    def get_files(self, path, name_pattern, recursive, **user_args):
        return self.obj_store.get_files(path, name_pattern, recursive,
                                        **user_args)

    def open(self, path, opts):
        return self.obj_store.open(path, opts)

    def schema_discover(self, path, args, retry_on_error):
        return self.obj_store.schema_discover(path, args, retry_on_error)
