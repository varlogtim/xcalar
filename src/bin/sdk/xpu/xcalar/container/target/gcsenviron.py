import logging

from google.cloud.storage.client import Client

from xcalar.container.connectors.gcs import GCSConnector
from xcalar.container.connectors.object_store import ObjectStoreConnector
import xcalar.container.target.base as target

logger = logging.getLogger("xcalar")


@target.register(
    name="Google Cloud Storage Account with Environmental Credentials",
    is_available=GCSConnector.is_available())
class GcsEnvironTarget(target.BaseTarget):
    """
    Uses the environmental setup to access Google Cloud Storage Account
    """

    def __init__(self, name, path, **kwargs):
        super(GcsEnvironTarget, self).__init__(name)
        client = Client()

        gcs_connector = GCSConnector(client)
        self.obj_store = ObjectStoreConnector(gcs_connector)

    def is_global(self):
        return True

    def get_files(self, path, name_pattern, recursive, **user_args):
        return self.obj_store.get_files(path, name_pattern, recursive)

    def open(self, path, opts):
        return self.obj_store.open(path, opts)
