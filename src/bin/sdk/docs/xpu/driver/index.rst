.. _xpu-drivers:

Export Drivers
==============

An export driver is a piece of ‘user’ written code which utilizes a full,
distributed Xcalar cluster to send the data from a table to some other
location, typically a Data Target registered with the cluster.

Export drivers are run within the XPU App infrastructure on all cores on all
Xcalar nodes. A driver should utilize the XPU SDK to access table data and
coordinate with its running sister instances where needed. The XPU SDK provides
functionality which communicates with its XCE host, such as for fetching the
data in a table, as well as some functionality which is simply useful for many
different drivers, such as the math for fairly partitioning a distributed
table. The XPU SDK also includes functionality such as:

 * Access to DataTargets registered with the Xcalar cluster
 * Fairly partitioning a Xcalar Table across all XPUs running the Export Driver
 * Distributed barrier to allow XPUs to wait until all have reached the same
   point in the code

Sample Driver
-------------

.. code-block:: python

    import xcalar.container.driver.base as driver
    import xcalar.container.context as ctx
    import logging
    import os
    import json
    @driver.register_export_driver(name="single_csv")
    @driver.param(name="file_path",
                  type=driver.STRING,
                  desc="exported file name")
    @driver.param(name="header",
                  type=driver.BOOLEAN,
                  optional=True,
                  desc="whether to include a header line as the first "
                       "line of the file. Defaults to True.")
    # @driver.param(name="driver param 4", type=driver.TARGET,
    #              desc="test driver param4",
    #              optional=True, secret=True)
    def driver(table, file_path, header=True):
        # Get our list of columns from the table. Note that we don't
        # care about what the column names were in the original table,
        # only what the user has decided they would like to call these
        # columns in the exported format
        columns = [c["headerAlias"] for c in table.columns()]

        # This code runs on all XPUs on all nodes, but for only
        # creating a single file, we are going to do that from just
        # a single XPU. Thus, all other XPUs can now exit
        xpu_id = ctx.get_xpu_id()
        if xpu_id != 0:
            return

        # Let's create our directories first
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Now we create the file itself
        with open(file_path, "w") as f:
            # Let's create our CSV writer object. We're using mostly
            # default arguments here. The 'extrasaction=ignore' means
            # that any spurious fields not in 'columns' will simply be
            # ignored by the CSV writer
            writer = csv.DictWriter(f, columns, extrasaction="ignore")
            if header:
                writer.writeheader()
            # Now we create a row cursor across all the rows in the
            # table
            all_rows_cursor = table.all_rows()
            # We could iterate over this cursor and write each row
            # directly, but the writer object has highly performant
            # code for doing this type of batch processing and it is
            # all written in C, so we will defer to it
            writer.writerows(all_rows_cursor)

Table
-----
.. autoclass:: xcalar.container.table.Table()
    :members:


XpuCluster
----------
.. autofunction:: xcalar.container.cluster.get_running_cluster()
.. autoclass:: xcalar.container.cluster.XpuCluster()
    :members:


Driver Registration
-------------------
.. autofunction:: xcalar.container.driver.base.register_export_driver
.. autofunction:: xcalar.container.driver.base.param

