# Copyright 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

from xcalar.compute.localtypes.Workbook_pb2 import WorkbookScope
from xcalar.compute.localtypes.KvStore_pb2 import LookupRequest
from xcalar.compute.localtypes.KvStore_pb2 import DeleteKeyRequest
from xcalar.compute.localtypes.KvStore_pb2 import AddOrReplaceRequest
from xcalar.compute.localtypes.KvStore_pb2 import MultiAddOrReplaceRequest
from xcalar.compute.localtypes.KvStore_pb2 import AppendRequest
from xcalar.compute.localtypes.KvStore_pb2 import SetIfEqualRequest
from xcalar.compute.localtypes.KvStore_pb2 import ListRequest


class KvStore:
    def __init__(self, client, scope):
        self._client = client
        self._scope = scope

    @classmethod
    def global_(cls, client):
        scope = WorkbookScope()
        scope.globl.SetInParent()
        return cls(client, scope)

    @classmethod
    def workbook_by_name(cls, client, user_name, workbook_name):
        scope = WorkbookScope()
        scope.workbook.name.username = user_name
        scope.workbook.name.workbookName = workbook_name
        return cls(client, scope)

    def lookup(self, key):
        req = LookupRequest()
        req.key.scope.CopyFrom(self._scope)
        req.key.name = key
        res = self._client._kvstore_service.lookup(req)
        return res.value.text

    def add_or_replace(self, key, value, persist):
        req = AddOrReplaceRequest()
        req.key.scope.CopyFrom(self._scope)
        req.key.name = key
        req.persist = persist
        req.value.text = value
        res = self._client._kvstore_service.addOrReplace(req)

    def multi_add_or_replace(self, kv_dict, persist):
        # XXX Only support session scope kvstore
        if self._scope.WhichOneof("specifier") != "workbook":
            raise TypeError("multi_add_or_replace operation is only supported"
                            + " on session scoped kvstore")
        if not isinstance(kv_dict, dict):
            raise TypeError(
                "kv_pairs should be a dict, not '{}'".format(kv_dict))

        req = MultiAddOrReplaceRequest()
        req.persist = persist
        req.scope.CopyFrom(self._scope)
        for k, v in kv_dict.items():
            req.keys.extend([k])
            value = req.values.add()
            value.text = v
        res = self._client._kvstore_service.multiAddOrReplace(req)

    def append(self, key, suffix):
        req = AppendRequest()
        req.key.scope.CopyFrom(self._scope)
        req.key.name = key
        req.suffix = suffix
        res = self._client._kvstore_service.append(req)

    def set_if_equal(self,
                     persist,
                     compare_key,
                     compare_value,
                     replace_value,
                     secondary_key=None,
                     secondary_value=None):
        req = SetIfEqualRequest()
        req.scope.CopyFrom(self._scope)
        req.persist = persist
        req.keyCompare = compare_key
        req.valueCompare = compare_value
        req.valueReplace = replace_value
        if secondary_key:
            req.keySecondary = secondary_key
        if secondary_value:
            req.valueSecondary = secondary_value
        res = self._client._kvstore_service.setIfEqual(req)

    def delete(self, key):
        req = DeleteKeyRequest()
        req.key.scope.CopyFrom(self._scope)
        req.key.name = key
        res = self._client._kvstore_service.deleteKey(req)

    def list(self, regex=".*"):
        req = ListRequest()
        req.scope.CopyFrom(self._scope)
        req.keyRegex = regex
        res = self._client._kvstore_service.list(req)
        return res.keys
