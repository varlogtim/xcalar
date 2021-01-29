Export Drivers
==============

An export driver is a piece of ‘user’ written code which utilizes a full,
distributed Xcalar cluster to send the data from a table to some other
location, typically a Data Target registered with the cluster.

This is the reference for the client side of drivers, allowing querying of the
drivers. If you want information about authoring drivers, see
:ref:`xpu-drivers`.

.. autoclass:: xcalar.external.driver.Driver()
    :members:
