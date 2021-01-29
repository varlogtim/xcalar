import xcalar.container.driver.base as driver
import xcalar.container.context as ctx
import logging
import os
import json

logger = logging.getLogger("xcalar")


@driver.register_export_driver(name="darrin_json_driver")
@driver.param(
    name="dir_path",
    type=driver.STRING,
    desc="export directory into which to drop our json files")
@driver.param(
    name="file_base",
    type=driver.STRING,
    desc="base file name to be used in formatting the output files")
# @driver.param(name="driver param 4", type=driver.TARGET,
#              desc="test driver param4",
#              optional=True, secret=True)
def driver(table, dir_path, file_base):
    xpu_id = ctx.get_xpu_id()

    file_name = "{}-{}.json".format(file_base, xpu_id)
    path = os.path.join(dir_path, file_name)

    # Let's create our directories first
    os.makedirs(dir_path, exist_ok=True)

    rows = list(table.partitioned_rows())
    # Now we create the file itself
    with open(path, "w") as f:
        json.dump(rows, f)
