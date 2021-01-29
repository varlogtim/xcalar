import io
import os
import logging

from .util import mtime_from_datetime
from .object_store import ObjectStoreImpl, KeyEntry, BucketEntry

logger = logging.getLogger("xcalar")


class GCSConnector(ObjectStoreImpl):
    @staticmethod
    def is_available():
        try:
            import google.cloud.storage    # noqa: F401
        except ImportError:
            return False
        return True

    def __init__(self, client):
        if not GCSConnector.is_available():
            raise ImportError("No module named google.cloud.storage")

        self._client = client

        self._bucket_cache = {}

    def _get_bucket(self, bucket_name):
        if bucket_name not in self._bucket_cache:
            self._bucket_cache[bucket_name] = self._client.get_bucket(
                bucket_name)
        return self._bucket_cache[bucket_name]

    def list_buckets(self):
        for bucket in self._client.list_buckets():
            yield BucketEntry(
                name=bucket.name,
                mtime=mtime_from_datetime(bucket.time_created))

    def list_keys(self, bucket_name, prefix, delimiter):
        bucket = self._get_bucket(bucket_name)

        list_args = {}
        if prefix:
            list_args["prefix"] = prefix

        if delimiter:
            list_args["delimiter"] = delimiter

        while True:
            blob_list = bucket.list_blobs(**list_args)

            prefix_list = blob_list.prefixes

            for blob in blob_list:
                yield KeyEntry(
                    name=blob.name,
                    is_prefix=False,
                    mtime=mtime_from_datetime(blob.updated),
                    size=blob.size)

            for prefix in prefix_list:
                yield KeyEntry(name=prefix, is_prefix=True, mtime=0, size=0)

            # Clear continuation token; this list_args might get used again
            list_args.pop("page_token", None)
            if blob_list.next_page_token:
                list_args["page_token"] = blob_list.next_page_token
                continue
            break

    def open(self, bucket_name, key, opts):
        if "w" in opts:
            raise ValueError("Write files not implemented for GCS")

        blob = self._get_bucket(bucket_name).get_blob(key)

        # XXX This is not streaming; the below is
        full_file = io.BytesIO(blob.download_as_string())
        return full_file
        # XXX we cannot use this until we get google-cloud-storage>=1.9.0
        return self.GCSFile(blob, blob.size)

    class GCSFile(io.RawIOBase):
        def __init__(self, blob, size):
            self._blob = blob
            self._size = size
            self._pos = 0

        def readable(self):
            return True

        def seekable(self):
            return True

        def writeable(self):
            return False

        def tell(self):
            return self._pos

        def seek(self, offset, whence=os.SEEK_SET):
            if whence == os.SEEK_CUR:
                new_pos = self._pos + offset
            elif whence == os.SEEK_END:
                new_pos = self._size + offset
            elif whence == os.SEEK_SET:
                new_pos = offset
            else:
                raise ValueError("Whence can only be 0, 1, or 2")

            if new_pos > self._size:
                raise ValueError("Cannot seek beyond end of file")

            if new_pos < 0:
                raise ValueError("Cannot seek before start of file")

            self._pos = new_pos

            return self._pos

        def readinto(self, b):
            chunk = self._read_bytes(len(b))
            b[:len(chunk)] = chunk
            return len(chunk)

        def _read_bytes(self, size):
            remaining = self._size - self._pos

            size = max(0, min(remaining, size))

            if size == 0:
                return b''

            # end is inclusive, so size - 1
            data = self._blob.download_as_string(
                start=self._pos, end=self._pos + size - 1)

            self._pos += len(data)

            return data
