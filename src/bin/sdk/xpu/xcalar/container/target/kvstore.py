import io

from xcalar.external.client import Client
from xcalar.container.connectors.util import File

import xcalar.container.target.base as target


@target.internal
@target.register(name="KVStore browser")
class KvStoreTarget(target.BaseTarget):
    """
    Browser the global kvstore of your local cluster
    """

    def __init__(self, name, path, **kwargs):
        super(KvStoreTarget, self).__init__(name)
        client = Client()
        self._global_store = client.global_kvstore()

    def is_global(self):
        return True

    def get_files(self, path, name_pattern, recursive, **user_args):
        if recursive:
            raise NotImplementedError()
        keys = self._global_store.list()

        path = path.lstrip('/')

        dirs = []

        selected_keys = [k for k in keys if k.startswith(path)]

        return [
            File(path=k, relPath=k, isDir=False, size=1, mtime=int(0))
            for k in selected_keys
        ]

    def open(self, path, opts):
        path = path.lstrip('/')
        value = self._global_store.lookup(path)
        b = io.BytesIO(value.encode("utf-8"))
        return b
