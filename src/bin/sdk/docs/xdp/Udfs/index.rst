.. |br| raw:: html

    <br />

User Defined Function (UDF) Modules
===================================

.. autoclass:: xcalar.external.udf.UDF(client, workbook, udf_module_name)
    :members:

Examples
--------

Create a UDF Module in a workbook
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. literalinclude:: ../../../../tests/pyTestNew/test_sdk_udf.py
   :start-after: [START create_udf_module_in_workbook]
   :end-before: [END create_udf_module_in_workbook]
   :dedent: 1

Create a UDF Module shared between users/workbooks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. literalinclude:: ../../../../tests/pyTestNew/test_sdk_udf.py
   :start-after: [START create_shared_udf_module]
   :end-before: [END create_shared_udf_module]
   :dedent: 1

Delete a UDF Module
^^^^^^^^^^^^^^^^^^^
.. literalinclude:: ../../../../tests/pyTestNew/test_sdk_udf.py
   :start-after: [START delete_udf_module]
   :end-before: [END delete_udf_module]
   :dedent: 1

Update a UDF Module
^^^^^^^^^^^^^^^^^^^
.. literalinclude:: ../../../../tests/pyTestNew/test_sdk_udf.py
   :start-after: [START update_udf_module]
   :end-before: [END update_udf_module]
   :dedent: 1
