import pytest

from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException
from xcalar.external.LegacyApi.Udf import Udf

simple_driver = """
import xcalar.container.driver.base as driver

@driver.register_export_driver(name="test export driver")
@driver.param(name="driver param 1", type=driver.INTEGER, desc="test driver param1")
@driver.param(name="driver param 2", type=driver.STRING, desc="test driver param2",
              optional=True)
@driver.param(name="driver param 3", type=driver.BOOLEAN, desc="test driver param3",
              secret=True)
@driver.param(name="driver param 4", type=driver.TARGET, desc="test driver param4",
              optional=True, secret=True)
def driver():
    return

def placeholder():
    return 1 # this is needed because otherwise this UDF module just gets swallowed
"""

builtin_shadow_driver = """
import xcalar.container.driver.base as driver

@driver.register_export_driver(name="single_csv")
def driver():
    return

def placeholder():
    return 1 # this is needed because otherwise this UDF module just gets swallowed
"""

# XXX we might not want to keep the excel export driver as a builtin
BUILTIN_DRIVERS = [
    "single_csv", "multiple_csv", "legacy_udf", "snapshot_export_driver",
    "do_nothing", "fast_csv", "snapshot_parquet", "snowflake_export"
]

HIDDEN_DRIVERS = [
    "snapshot_export_driver", "do_nothing", "snapshot_parquet", "snapshot_csv"
]


@pytest.fixture(scope="module")
def udf(workbook):
    xc_api = XcalarApi()
    xc_api.setSession(workbook)

    udf_lib = Udf(xc_api)
    return udf_lib


def test_add_delete_list(client, udf):
    # XXX try changing this driver name; won't work for now
    driver_name = "test export driver"
    expected_params = [
        {
            "name": "driver param 1",
            "description": "test driver param1",
            "type": "integer",
            "optional": False,
            "secret": False
        },
        {
            "name": "driver param 2",
            "description": "test driver param2",
            "type": "string",
            "optional": True,
            "secret": False
        },
        {
            "name": "driver param 3",
            "description": "test driver param3",
            "type": "boolean",
            "optional": False,
            "secret": True
        },
        {
            "name": "driver param 4",
            "description": "test driver param4",
            "type": "target",
            "optional": True,
            "secret": True
        },
    ]

    udf_name = "/sharedUDFs/driverTestUdf"
    udf.addOrUpdate(udf_name, simple_driver)
    driver = client.get_driver(driver_name)

    assert driver.name == driver_name
    assert driver.params == expected_params

    assert any([
        d.name == driver_name and d.params == expected_params
        for d in client.list_drivers()
    ])

    driver = client.get_driver(driver_name)
    assert driver.name == driver_name
    assert driver.params == expected_params

    udf.delete(udf_name)

    assert driver_name not in [d.name for d in client.list_drivers()]


def test_driver_list(client):
    drivers = client.list_drivers()
    param_fields = {
        "name": str,
        "description": str,
        "type": str,
        "optional": bool,
        "secret": bool,
    }

    # We should have the exact set of all builtin drivers
    builtin_drivers = [d for d in drivers if d.is_builtin]
    builtin_names = [d.name for d in builtin_drivers]
    assert set(builtin_names) == set(BUILTIN_DRIVERS) - set(HIDDEN_DRIVERS)

    # All drivers should have these fields
    for d in builtin_drivers:
        assert d.name != ""
        assert d.description != ""
        assert type(d.is_builtin) == bool
        assert str(d) != ""
        assert type(d.params) == list

        for param in d.params:
            # Each driver parameter should have this set of fields
            assert set(param.keys()) == set(
                ["name", "description", "type", "optional", "secret"])
            # Let's make sure the parameter's metadata fields are valid
            for k, v in param.items():
                expected_key_type = param_fields[k]
                assert type(v) == expected_key_type
                if expected_key_type == str:
                    # things like name, description, type shouldn't be empty
                    assert v != ""


def test_builtin_safeguards(client, udf):
    # Get the name of some builtin driver
    builtin_driver_name = BUILTIN_DRIVERS[0]
    udf_name = "/sharedUDFs/driverTestUdf"

    # Can't add a driver with a builtin's name
    with pytest.raises(XcalarApiStatusException) as exc:
        udf.addOrUpdate(udf_name, builtin_shadow_driver)
    assert "builtin" in exc.value.result.output.hdr.log
