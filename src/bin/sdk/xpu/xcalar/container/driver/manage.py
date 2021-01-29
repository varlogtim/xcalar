import os
import importlib
import logging

from xcalar.container.xce_config import get_config
from .base import extract_drivers, extract_drivers_from_source, BUILTIN_DRIVERS

# Keep this up to date with the XCE side string
SHARED_UDFS_PATH = "sharedUDFs/python"

KEY_FORMAT_STR = "xce.export_driver.{driver_name}"

logger = logging.getLogger("xcalar")
logger.setLevel(logging.INFO)


def gen_drivers():
    # builtins
    for mod_name in BUILTIN_DRIVERS:
        full_mod_name = "xcalar.container.driver.builtins.{}".format(mod_name)
        module = importlib.import_module(full_mod_name)
        yield from extract_drivers(module)

    # drivers in uploaded shared UDFs
    yield from _gen_shared_drivers()


def _shared_udfs_path():
    config = get_config()
    return os.path.join(config.xcalar_root_path, SHARED_UDFS_PATH)


def _gen_shared_drivers():
    shared_udf_dir = _shared_udfs_path()
    try:
        files = os.listdir(shared_udf_dir)
    except FileNotFoundError:
        # There's no shared UDFs directory; that's fine, the user just doesn't
        # have any shared UDFs then.
        files = []

    for fn in files:
        if not fn.endswith(".py"):
            continue
        file_base = fn[:-3]
        path = os.path.join(shared_udf_dir, fn)
        try:
            with open(path) as f:
                source = f.read()
        except OSError:
            logger.exception()
            pass
        yield from extract_drivers_from_source(path, module_source=source,
                                               module_name=file_base)


def list_driver_dicts():
    for driver in gen_drivers():
        if not driver.is_hidden:
            driver_dict = {
                "name": driver.name,
                "description": driver.description,
                "params": driver.param_dict_list(),
                "isBuiltin": driver.is_builtin
            }
            yield driver_dict


def get_driver(driver_name):
    # XXX This is a little bit hacky and assumes that the builtin driver name
    # is the same as the module name and there's only 1 driver in the module
    if driver_name in BUILTIN_DRIVERS:
        full_mod_name = "xcalar.container.driver.builtins.{}".format(
            driver_name)
        module = importlib.import_module(full_mod_name)
        driver = next(extract_drivers(module))
        return driver
    for driver in _gen_shared_drivers():
        if driver.name == driver_name:
            return driver
