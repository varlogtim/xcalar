Xcalar Data Platform
=====================

The Xcalar Data Platform (XDP) SDK provides users with the ability to use Xcalar from
outside of Xcalar Design.

    **Note**: *This SDK is still in beta , and thus a lot of the
    functionality found in Xcalar Design may not yet be available in the SDK.
    For instance, one cannot use operators such as join/map through the
    XDP SDK. This functionality should come out in future releases.*

    *The XDP SDK should therefore be used for more administrative tasks
    such as cluster management, scheduling, viewing result sets, importing datasets
    and adding/dropping workbooks, to name a few.*

.. raw:: html

    <h3>Getting Started</h3>

Install the ``xcalar-external`` libraray using pip:

.. code-block:: console

    $ pip install xcalar-external

.. toctree::
   :maxdepth: 2
   :hidden:

   Client/index.rst
   workbook/index.rst
   Session/index.rst
   Datasets/index.rst
   tables/index.rst
   imd/index.rst
   Dataflow/index.rst
   DataTargets/index.rst
   Driver/index.rst
   DatasetBuilder/index.rst
   Udfs/index.rst

.. raw:: html

    <h3>Examples</h3>

.. raw:: html

    <h4>Get Statistics for all nodes in Cluster</h4>
.. literalinclude:: ../../../tests/pyTestNew/test_client_new.py
   :start-after: [START get_metrics]
   :end-before: [END get_metrics]

.. raw:: html

    <h4>Check and Set Config Parameter</h4>
    The following code checks the parameter value of SwapUsagePercent, and updates it to 90 if necessary.

.. literalinclude:: ../../../tests/pyTestNew/test_client_new.py
   :start-after: [START get_and_set_config_param]
   :end-before: [END get_and_set_config_param]

.. raw:: html

    <h4>Using UDF in dataflow</h4>

.. literalinclude:: ../../../tests/pyTestNew/sdk_samples/test_udf_demo.py
   :start-after: [START udf_demo]
   :end-before: [END udf_demo]

.. raw:: html

    <h4>Using streaming UDF to parse dattaset</h4>

.. literalinclude:: ../../../tests/pyTestNew/sdk_samples/test_streaming_udf_demo.py
   :start-after: [START streaming_udf]
   :end-before: [END streaming_udf]

.. raw:: html

    <h4>Setting data target to import data from different storage platform</h4>

.. literalinclude:: ../../../tests/pyTestNew/sdk_samples/test_data_target_demo.py
   :start-after: [START data_target]
   :end-before: [END data_target]

.. raw:: html

    <h4>Export table with a driver</h4>

.. literalinclude:: ../../../tests/pyTestNew/sdk_samples/test_export_driver_demo.py
   :start-after: [START export_driver]
   :end-before: [END export_driver]


