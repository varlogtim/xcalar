import os
import io
from itertools import groupby
import logging

from .util import mtime_from_datetime
from .object_store import ObjectStoreImpl, KeyEntry, BucketEntry

logger = logging.getLogger("xcalar")

AzBlobAvailable = False
try:
    import azure.storage.blob.models
    AzBlobAvailable = True
except ImportError:
    pass


class AzBlobConnector(ObjectStoreImpl):
    @staticmethod
    def is_available():
        return AzBlobAvailable

    def __init__(self, block_blob_service):
        if not AzBlobAvailable:
            raise ImportError("No module named azure.storage")

        self.blob_service = block_blob_service

    def list_buckets(self):
        for container in self.blob_service.list_containers():
            yield BucketEntry(
                name=container.name,
                mtime=mtime_from_datetime(container.properties.last_modified))

    def list_keys(self, container_name, prefix, delimiter):
        list_args = {}
        if prefix:
            list_args["prefix"] = prefix

        if delimiter:
            list_args["delimiter"] = delimiter

        object_stream = self.blob_service.list_blobs(container_name,
                                                     **list_args)

        # there is an azure bug where there are sometimes duplicates here
        # https://github.com/Azure/azure-storage-net/issues/396 this issue
        # incorrectly says the problem should already be resolved
        object_stream = [
            next(group) for _, group in groupby(
                list(object_stream), lambda blob: blob.name)
        ]

        for obj in object_stream:
            if isinstance(obj, azure.storage.blob.models.BlobPrefix):
                yield KeyEntry(name=obj.name, is_prefix=True, mtime=0, size=0)
            elif isinstance(obj, azure.storage.blob.models.Blob):
                yield KeyEntry(
                    name=obj.name,
                    is_prefix=False,
                    mtime=mtime_from_datetime(obj.properties.last_modified),
                    size=obj.properties.content_length)
            else:
                raise ValueError("invalid Azure blob type '{}'".format(
                    type(obj)))

    def open(self, container_name, key, opts):
        if "w" in opts:
            raise ValueError(
                "Write files not implemented for Azure Blob Storage")

        size = self.blob_service.get_blob_properties(
            container_name, key).properties.content_length

        return self.AzBlobFile(self.blob_service, container_name, key, size)

    class AzBlobFile(io.RawIOBase):
        def __init__(self, blob_service, container_name, blob_path, size):
            self.blob_service = blob_service
            self.container_name = container_name
            self.blob_path = blob_path
            self.pos = 0
            self.timeout = 2 * 60    # 2 min
            self.size = size

        def readable(self):
            return True

        def writeable(self):
            return False

        def seekable(self):
            return True

        def _raw_read(self, num_bytes):
            num_bytes = min(num_bytes, self.size - self.pos)
            if num_bytes > 0:
                blob = self.blob_service.get_blob_to_bytes(
                    self.container_name,
                    self.blob_path,
                    start_range=self.pos,
                    end_range=self.pos + num_bytes -
                    1,    # end_range is inclusive
                    timeout=self.timeout)
                content = blob.content
                self.pos += len(content)
            else:
                content = bytes()
            return content

        def readinto(self, b):
            b_len = len(b)
            chunk = self._raw_read(b_len)
            if len(chunk) > 0:
                b[:len(chunk)] = chunk
            return len(chunk)

        def seek(self, offset, whence=os.SEEK_SET):
            desired_pos = None
            if whence == os.SEEK_SET:
                desired_pos = offset
            elif whence == os.SEEK_CUR:
                desired_pos = self.pos + offset
            elif whence == os.SEEK_END:
                desired_pos = self.size + offset

            if desired_pos > self.size:
                raise ValueError(
                    "Cannot seek past the end of an Azure Blob (seek to {})".
                    format(desired_pos))

            if desired_pos < 0:
                raise ValueError(
                    "Cannot seek before the start of an Azure Blob (seek to {})"
                    .format(desired_pos))

            self.pos = desired_pos
            return self.pos
