from azure.storage.blob import BlockBlobService

from xcalar.container.connectors.azblob import AzBlobConnector
from xcalar.container.connectors.object_store import ObjectStoreConnector
import xcalar.container.target.base as target

import logging
logger = logging.getLogger("xcalar")


@target.register(
    name="azure blob storage account with sas token",
    is_available=AzBlobConnector.is_available())
@target.param("account_name", "Storage Account name")
@target.param("sas_token", "Storage Account SAS Token")
class AzBlobFullAccountTarget(target.BaseTarget):
    """
    Specifies the credentials to an Azure Storage Account using a generated
    Shared Access Signature Token.
    """

    def __init__(self, name, path, account_name, sas_token, **kwargs):
        super(AzBlobFullAccountTarget, self).__init__(name)

        # XXX fixup the token to be valid; something may be wrong here
        sas_token = "?sr=b&" + sas_token[1:]

        blob_service = BlockBlobService(
            account_name=account_name, sas_token=sas_token)

        azblob_connector = AzBlobConnector(blob_service)
        self.obj_store = ObjectStoreConnector(azblob_connector)

    def is_global(self):
        return True

    def get_files(self, path, name_pattern, recursive, **user_args):
        return self.obj_store.get_files(path, name_pattern, recursive)

    def open(self, path, opts):
        return self.obj_store.open(path, opts)
