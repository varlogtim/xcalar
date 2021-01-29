import io
import logging

from .util import File, match

PAGED_LIST_MIN_SIZE = 3000
DELIMITER = "/"

logger = logging.getLogger("xcalar")


def _relpath(path, start, isdir):
    path_components = path.split(DELIMITER)
    start_components = start.split(DELIMITER)

    start_components = start_components[:-1]

    # S3 prefixes have a trailing slash by definition
    if isdir:
        path_components = path_components[:-1]

    ii = 0
    while (ii < len(path_components) and ii < len(start_components)
           and path_components[ii] == start_components[ii]):
        ii += 1

    if ii == len(path_components):
        return path_components[-1]
    else:
        return DELIMITER.join(path_components[ii:])


def _parse_obj_store_path(full_path):
    parts = full_path.lstrip('/').split(DELIMITER)
    bucket_name = parts[0]
    path = DELIMITER.join(parts[1:])

    return (bucket_name, path)


def _construct_obj_store_path(bucket_name, key):
    return "/{bucket_name}/{fullkey}".format(
        bucket_name=bucket_name, fullkey=key)


def _prepare_list_key_args(key_path, name_pattern, recursive):
    # XXX future improvement: I think that we could take the portion of
    # the beginning of the pattern up to the first meta character and
    # append that to the prefix. This would allow us to filter some of
    # the results at the AWS level and reduce the amount of list items
    # we need to iterate on. Imagine 1M keys, and pattern: foo*.csv,
    # if there are no files beginning with foo, we will have to look
    # through the entire list of files to determine that.
    if name_pattern == "":
        name_pattern = "*"

    prefix = None
    delimiter = None

    # Recursive lists only return keys and not prefixes.
    if recursive:
        if key_path:
            # When loading a 'directory' recursively, S3 needs to
            # have the '/' explicitly present. E.g.:
            #  a/a.txt
            #  ab/ab.txt
            # path='a', recursive=True
            # If we just use the prefix 'a', it will match 'ab'
            # Note that this means 'recursive' when specifying
            # a single key will not work.
            if not key_path.endswith(DELIMITER):
                key_path += DELIMITER
            prefix = key_path
    else:
        if key_path:
            prefix = key_path
        # Non-recursive file listing; we will have prefixes
        delimiter = DELIMITER
    return (key_path, name_pattern, prefix, delimiter)


class KeyEntry:
    def __init__(self, name, is_prefix, mtime, size):
        self.name = name
        self.is_prefix = is_prefix
        self.mtime = mtime
        self.size = size

    def to_file_obj(self, bucket_name, key_path):
        given_path = _construct_obj_store_path(bucket_name, key_path)

        full_path = _construct_obj_store_path(bucket_name, self.name)
        rel_path = _relpath(full_path, given_path, self.is_prefix)
        file_obj = File(
            path=full_path,
            relPath=rel_path,
            isDir=self.is_prefix,
            size=self.size,
            mtime=self.mtime)
        return file_obj


class BucketEntry:
    def __init__(self, name, mtime):
        self.name = name
        self.mtime = mtime

    def to_file_obj(self):
        file_obj = File(
            path=self.name,
            relPath=self.name,
            isDir=True,
            size=0,
            mtime=self.mtime)
        return file_obj


class ObjectStoreImpl:
    def list_buckets(self):
        raise NotImplementedError()

    def list_keys(self, bucket_name, prefix, delimiter):
        raise NotImplementedError()

    def open(self, bucket_name, key, opts):
        raise NotImplementedError()


class ObjectStoreConnector():
    def __init__(self, object_store):
        self.object_store = object_store

    # XXX Does method overloading make sense here? The return
    # types are different, so maybe not.
    def get_files_paged(self, path, name_pattern, recursive, token):
        """
        List files and page results in sizes of {page_size}
        """.format(page_size=(PAGED_LIST_MIN_SIZE + 1000))

        bucket_name, key_path = _parse_obj_store_path(path)
        # We don't need to page buckets. Max is 1000
        # https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
        if not bucket_name:
            if recursive:
                raise ValueError(
                    "Recursive search only available for a given bucket")
            file_objs = [
                b.to_file_obj() for b in self.object_store.list_buckets()
            ]
            return ("", file_objs)

        (key_path, name_pattern, prefix, delimiter) = _prepare_list_key_args(
            key_path, name_pattern, recursive)

        filtered_dirs = []
        filtered_files = []
        while (len(filtered_dirs) + len(filtered_files) < PAGED_LIST_MIN_SIZE):
            # list_size < PAGED_LIST_MIN_SIZE or,
            # PAGED_LIST_MIN_SIZE < list_size < PAGED_LIST_MIN_SIZE + 1000
            (next_token, keys) = self.object_store.list_keys_page(
                bucket_name, prefix, delimiter, token)

            for key in keys:
                file_obj = key.to_file_obj(bucket_name, key_path)
                if file_obj.isDir:
                    if match(name_pattern, recursive, file_obj):
                        filtered_dirs.append(file_obj)
                else:
                    if match(name_pattern, recursive, file_obj):
                        filtered_files.append(file_obj)

            if next_token == "":
                break
            token = next_token

        # Return all dirs first
        return ([*filtered_dirs, *filtered_files], next_token)

    def get_files(self, path, name_pattern, recursive, **user_args):
        """
        List the visible files matching name_pattern. Should return a "File"
        namedtuple.
        """
        bucket_name, key_path = _parse_obj_store_path(path)

        # Make S3 look like a hierarchical filesystem
        if not bucket_name:
            if recursive:
                raise ValueError(
                    "Recursive search only available for a given bucket")
            return [b.to_file_obj() for b in self.object_store.list_buckets()]

        (key_path, name_pattern, prefix, delimiter) = _prepare_list_key_args(
            key_path, name_pattern, recursive)

        keys = self.object_store.list_keys(bucket_name, prefix, delimiter)

        # Notice we use the modified key_path (with a trailing '/' for
        # directories). The to_file_obj method assumes this has been done
        file_objs = (k.to_file_obj(bucket_name, key_path) for k in keys)

        filtered_objs = [
            fo for fo in file_objs if match(name_pattern, recursive, fo)
        ]
        return filtered_objs

    def open(self, path, opts):
        bucket_name, key = _parse_obj_store_path(path)

        if key == '':
            msg = 'Invalid path specified. Example: /bucket/path/to/file.data'
            raise ValueError(msg)

        raw_file = self.object_store.open(bucket_name, key, opts)

        if opts == 'rb':
            buffered_file = io.BufferedReader(raw_file, 10 * 2**20)
            return buffered_file
        else:
            return raw_file

    def schema_discover(self, path, args, retry_on_error):
        (bucket, key) = _parse_obj_store_path(path)
        return self.object_store.schema_discover(bucket, key, args,
                                                 retry_on_error)

    def _call_kinesis(self, path):
        (bucket, key) = _parse_obj_store_path(path)
        return self.object_store._call_kinesis(bucket, key)

    def _get_temp_path(self, bucket, key):
        return _construct_obj_store_path(bucket, key)

    def delete(self, path):
        (bucket, key) = _parse_obj_store_path(path)
        self.object_store.delete(bucket, key)
