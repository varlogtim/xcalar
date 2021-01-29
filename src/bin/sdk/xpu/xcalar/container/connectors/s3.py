import io
import os
import time
import logging

from .util import mtime_from_datetime
from .object_store import ObjectStoreImpl, KeyEntry, BucketEntry

logger = logging.getLogger("xcalar")

# https://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html
S3_MAX_NUM_PARTS = 10000

# https://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html
S3_MAX_PART_SIZE = 5 * 10**9    # 5 GB

# This determines how big a part is before we submit it as one of our parts for
# the multi-part upload.
MULTI_PART_BUF_SIZE = 100 * 2**20    # 100 MB
ROUGH_MAX_FILE_SIZE = S3_MAX_NUM_PARTS * MULTI_PART_BUF_SIZE


class S3Connector(ObjectStoreImpl):
    @staticmethod
    def is_available():
        try:
            import boto3    # noqa: F401,E402
        except ImportError:
            return False
        return True

    def __init__(self, s3):
        """s3 is the boto3 resource for s3 configured from the appropriate session"""
        if not S3Connector.is_available():
            raise ImportError("No module named boto3")

        self._s3 = s3

    def list_buckets(self):
        for bucket in self._s3.buckets.all():
            yield BucketEntry(
                name=bucket.name,
                mtime=mtime_from_datetime(bucket.creation_date))

    def list_keys_page(self, bucket_name, prefix, delimiter,
                       continuation_token):
        """
        Return a single page of file item results, with next_token if needed.
        """
        # At some point we might want to rethink this class.
        list_args = {"Bucket": bucket_name}

        if prefix:
            list_args["Prefix"] = prefix
        if delimiter:
            list_args["Delimiter"] = delimiter
        if continuation_token:
            list_args["ContinuationToken"] = continuation_token

        try:
            list_dict = self._s3.meta.client.list_objects_v2(**list_args)
        except self._s3.meta.client.exceptions.NoSuchBucket:
            raise RuntimeError("No such bucket '{}'".format(bucket_name))

        keys = []
        for prefix_obj in list_dict.get("CommonPrefixes", []):
            keys.append(
                KeyEntry(
                    name=prefix_obj["Prefix"], is_prefix=True, mtime=0,
                    size=0))

        for s3_obj in list_dict.get("Contents", []):
            keys.append(
                KeyEntry(
                    name=s3_obj["Key"],
                    is_prefix=False,
                    mtime=mtime_from_datetime(s3_obj["LastModified"]),
                    size=s3_obj["Size"]))

        next_token = ""    # Must be string
        if list_dict["IsTruncated"]:
            next_token = list_dict["NextContinuationToken"]
        return (next_token, keys)

    def list_keys(self, bucket_name, prefix, delimiter):
        list_args = {
            "Bucket": bucket_name,
        }

        if prefix:
            list_args["Prefix"] = prefix

        if delimiter:
            list_args["Delimiter"] = delimiter

        while True:
            try:
                # We are using the low level 'client' APIs here because that
                # seems to be the only way to get prefix information
                list_dict = self._s3.meta.client.list_objects_v2(**list_args)
            # this exception is weird https://github.com/boto/boto3/issues/1195
            except self._s3.meta.client.exceptions.NoSuchBucket:
                raise RuntimeError("No such bucket '{}'".format(bucket_name))

            for s3_obj in list_dict.get("Contents", []):
                yield KeyEntry(
                    name=s3_obj["Key"],
                    is_prefix=False,
                    mtime=mtime_from_datetime(s3_obj["LastModified"]),
                    size=s3_obj["Size"])

            for prefix_obj in list_dict.get("CommonPrefixes", []):
                yield KeyEntry(
                    name=prefix_obj["Prefix"], is_prefix=True, mtime=0, size=0)

            # Clear continuation token; this list_args might get used again
            list_args.pop("ContinuationToken", None)
            if list_dict["IsTruncated"]:
                list_args["ContinuationToken"] = list_dict[
                    "NextContinuationToken"]
                continue
            break

    def open(self, bucket_name, key, opts):
        s3_obj = self._s3.Object(bucket_name, key)

        if opts == 'rb':
            return S3ReadableFile(s3_obj)
        elif opts == 'wb':
            return S3WritableStream(s3_obj)
        elif opts == 's3select':
            return S3SelectReadableStream(self._s3, bucket_name, key)
        else:
            raise ValueError("Unknown file mode {}".format(opts))

    def delete(self, bucket, key):
        self._s3.meta.client.delete_object(Bucket=bucket, Key=key)


class S3ReadableFile(io.RawIOBase):
    def __init__(self, s3_obj):
        self._s3_obj = s3_obj
        self._pos = 0
        self._size = None

    def readable(self):
        return True

    def seekable(self):
        return True

    def writeable(self):
        return False

    def tell(self):
        return self._pos

    def _obj_size(self):
        if self._size is None:
            self._size = self._s3_obj.content_length
        return self._size

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_CUR:
            new_pos = self._pos + offset
        elif whence == os.SEEK_END:
            new_pos = self._obj_size() + offset
        elif whence == os.SEEK_SET:
            new_pos = offset
        else:
            raise ValueError("Whence can only be 0, 1, or 2")

        if new_pos > self._obj_size():
            raise ValueError("Cannot seek beyond end of file")

        if new_pos < 0:
            raise ValueError("Cannot seek before start of file")

        self._pos = new_pos

        return self._pos

    def readinto(self, b):
        num_retries = 6
        for try_num in range(num_retries):
            try:
                chunk = self._read_bytes(len(b))
                break
            except Exception as e:
                if 'IncompleteRead' in str(e) and try_num != num_retries - 1:
                    time.sleep(2**try_num)
                    logger.error(
                        "S3 IncompleteRead, try_num: {}".format(try_num))
                    continue
                else:
                    raise
        b[:len(chunk)] = chunk
        return len(chunk)

    def _read_bytes(self, req_bytes):
        """Get the actual data as requested by the caller

        req_bytes is the number of requested bytes; the actual returned number
                may be smaller if the end of the file is reached
        """
        remaining = self._obj_size() - self._pos

        size = max(0, min(remaining, req_bytes))

        if size == 0:
            return b''

        # https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
        # bytes={start}-{end}, end is inclusive! ;)
        # https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
        byte_range = 'bytes={}-{}'.format(self._pos, self._pos + size - 1)

        obj = self._s3_obj.get(Range=byte_range)
        data = obj["Body"].read()

        self._pos += len(data)

        return data


class S3WritableStream(io.BufferedIOBase):
    def __init__(self, s3_obj):
        self._object = s3_obj
        self._buf = io.BytesIO()
        self._mp = None
        self._parts = []
        self._total_size = 0

    def close(self):
        if self._buf.tell():
            self._upload_part()

        if self._mp:
            self._complete_mp()
        else:
            assert not self._parts, "we must not have parts if the multipart is empty"
            self._object.put(b'')

    def readable(self):
        return False

    def seekable(self):
        return False

    def writeable(self):
        return True

    def tell(self):
        # amount of data uploaded to s3 + current buffer length
        return self._total_size + self._buf.tell()

    def write(self, b):
        if not isinstance(b, bytes):
            raise TypeError("argument must be of type bytes, not {}".format(
                type(b)))
        self._buf.write(b)

        if self._buf.tell() > MULTI_PART_BUF_SIZE:
            self._upload_part()

    def _start_mp(self):
        """Starts a multi-part s3 upload"""
        assert self._mp is None
        self._mp = self._object.initiate_multipart_upload()

    def _abort_mp(self):
        if self._mp:
            self._mp.abort()
            self._mp = None

    def _complete_mp(self):
        assert self._mp
        self._mp.complete(MultipartUpload={"Parts": self._parts})
        self._mp = None

    def _upload_part(self):
        if not self._mp:
            self._start_mp()
        # part_num starts at 1
        part_num = len(self._parts) + 1
        buf_size = self._buf.tell()
        if part_num >= S3_MAX_NUM_PARTS:
            raise RuntimeError(
                "file {} too large ({}>~{}); adjust multipart buffer size".
                format(self._object.key, self._total_size + buf_size,
                       ROUGH_MAX_FILE_SIZE))
        if buf_size > S3_MAX_PART_SIZE:
            raise RuntimeError("file {} part {} too large ({} > {})".format(
                self._object.key, part_num, buf_size, S3_MAX_PART_SIZE))
        part = self._mp.Part(part_num)
        self._buf.seek(0)
        upload = part.upload(Body=self._buf)
        self._parts.append({"ETag": upload["ETag"], "PartNumber": part_num})

        # This is actually cheaper than clearing the old one
        # https://stackoverflow.com/a/433082https://stackoverflow.com/a/43308299
        self._buf = io.BytesIO()
        self._total_size += buf_size

    # Dunders for flushing appropriately
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # We encountered some error
            self._abort_mp()
        else:
            try:
                self.close()
            except Exception:
                # if we have an issue closing, we still want to cancel
                self._abort_mp()
                raise


class S3SelectReadableStream():
    def __init__(self, s3, bucket, key):
        self._s3 = s3
        self._bucket = bucket
        self._key = key

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # XXX Might need to close stream.
        return

    def _bytes(self, input_serial, expression, output_serial=None):
        if output_serial is None:
            output_serial = {'JSON': {'RecordDelimiter': '\n'}}

        select_kwargs = {
            'Bucket': self._bucket,
            'Key': self._key,
            'ExpressionType': 'SQL',
            'Expression': expression,
            'OutputSerialization': output_serial,
            'InputSerialization': input_serial
        }
        ret = self._s3.meta.client.select_object_content(**select_kwargs)
        http_code = ret['ResponseMetadata']['HTTPStatusCode']
        if http_code != 200:
            msg = (f"s3 select failure: HTTP({http_code}) "
                   f"path: {self._bucket}/{self._key}")
            logger.error(msg)
            raise RuntimeError(msg)

        event_stream = ret['Payload']
        for event in event_stream:
            # XXX Need to return pointers to positions in source data.
            if 'Records' in event:
                yield event['Records']['Payload']

    def rows(self, input_serial, query, output_serial=None):
        last_hunk = b''
        for data in self._bytes(input_serial, query, output_serial):
            # XXX This might not be the best pattern.
            # Basically, we need a wrapper which reassembles hunks of bytes
            # and decodes them and returns strings. Revisit later.
            #
            # The data returned from the in_stream is hunked bytes of json
            # records terminated by a new line. If the hunk breaks a record
            # in two we need to add that hunk to the beginning of  the next
            # record. The very last record in the data will produce an empty
            # last_hunk, thus the assert at the bottom.
            #
            # Additionally, because the hunk boundary could be accross a multi-byte
            # character, we need to split the records while they are still
            # in a byte string, reassemble the hunks, then decode to string.
            data_rows_bytes = data.split(b'\n')
            num_rows = len(data_rows_bytes)
            data_rows_processed = 0

            for row_bytes in data_rows_bytes:
                # prepend first row with bytes of last row of previous data
                if data_rows_processed == 0:
                    row_bytes = last_hunk + row_bytes
                data_rows_processed += 1
                # save bytes of last row for beginning of the next data
                if data_rows_processed == num_rows:
                    last_hunk = row_bytes
                    continue
                row_bytes = row_bytes + b'\n'
                try:
                    yield row_bytes.decode('utf-8')
                except UnicodeDecodeError as decode_error:
                    raise ValueError("Unable to parse record")

        assert last_hunk == b''
