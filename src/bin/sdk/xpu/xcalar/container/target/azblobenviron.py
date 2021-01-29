import os
import logging

from azure.storage.blob import BlockBlobService

from xcalar.container.connectors.azblob import AzBlobConnector
from xcalar.container.connectors.object_store import ObjectStoreConnector
import xcalar.container.target.base as target

logger = logging.getLogger("xcalar")


@target.register(
    name="Azure Blob Storage Account with Environmental Credentials",
    is_available=AzBlobConnector.is_available())
class AzBlobEnvironTarget(target.BaseTarget):
    """
    Specifies the credentials to an Azure Storage Account using
    AZURE_STORAGE_ACCOUNT as the Storage Account and AZURE_STORAGE_KEY as
    the Storage Account Access Key.
    """

    def __init__(self, name, path, **kwargs):
        super(AzBlobEnvironTarget, self).__init__(name)
        blob_service = self._get_service()

        azblob_connector = AzBlobConnector(blob_service)
        self.obj_store = ObjectStoreConnector(azblob_connector)

    def _get_service(self):
        # Try the Access Key env var method
        account = os.getenv("AZURE_STORAGE_ACCOUNT", None)
        access_key = os.getenv("AZURE_STORAGE_KEY", None)
        sas_token = os.getenv("AZURE_STORAGE_SAS_TOKEN", None)

        if not account:
            raise ValueError(
                "Environment variable 'AZURE_STORAGE_ACCOUNT' not set")

        if not (access_key or sas_token):
            raise ValueError(
                "Environment variable 'AZURE_STORAGE_KEY' or 'AZURE_STORAGE_SAS_TOKEN' not set"
            )
        return BlockBlobService(
            account_name=account, account_key=access_key, sas_token=sas_token)

    def is_global(self):
        return True

    def get_files(self, path, name_pattern, recursive, **user_args):
        return self.obj_store.get_files(path, name_pattern, recursive)

    def open(self, path, opts):
        return self.obj_store.open(path, opts)
