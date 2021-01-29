# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import json


class Driver():
    """
    Drivers are a way for sophisticated users to have low-level control over
    the export process. A driver is a python function which is invoked in
    parallel across all nodes in the Xcalar cluster. This driver then should
    retrieve the table's data, potentially process it, and write it out as
    specified by the user. Drivers may specify parameters that they take from
    the user during export, such as the desired number of files,
    access credentials, or any other arbitrary information.

    **Attributes**

    The following attributes are properties associated with the Driver object.
    They cannot be modified.

    * **name** (:class:`str`) - Name of the Driver
    * **description** (:class:`str`) - Description of the Driver
    * **source** (:class:`str`) - Source code of the Driver
    * **params** (:class:`list`) - A list of the parameter descriptors
    * **is_builtin** (:class:`bool`) - Whether the driver is builtin to xcalar

    """

    def __init__(self, client, driver_name, description, params, is_builtin):
        self._client = client
        self._driver_name = driver_name
        self._description = description
        self._params = params
        self._is_builtin = is_builtin

    def _execute(self, workItem):
        return self._client._execute(workItem)

    @property
    def name(self):
        return self._driver_name

    @property
    def description(self):
        return self._description

    @property
    def params(self):
        return self._params

    @property
    def is_builtin(self):
        return self._is_builtin

    def _to_dict(self):
        details = dict()
        details['driver_name'] = self.name
        details['params'] = self.params
        return details

    def __str__(self):
        return json.dumps(self._to_dict())
