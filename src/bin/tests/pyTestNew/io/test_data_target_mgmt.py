import json
import pytest

from xcalar.compute.util.Qa import buildNativePath
from xcalar.external.LegacyApi.Dataset import CsvDataset
from xcalar.external.LegacyApi.XcalarApi import XcalarApi

targetTypes = [
    "shared", "s3environ", "s3fullaccount", "memory", "maprfullaccount",
    "maprimpersonation", "azblobfullaccount", "azblobenviron", "gcsenviron",
    "sharednothingsymm", "sharednothingsingle", "webhdfskerberos",
    "httpfskerberos", "webhdfsnokerberos", "httpfsnokerberos", "custom",
    "parquetds", "dsn", "kvstore", "imd_target", "urltarget", "snowflake"
]

defaultTargets = [
    "Default Shared Root",
]


def test_targ_sanity(client):
    targets = client.list_data_targets()


def test_sharednothingsingle_loadonefile(client):
    client.add_data_target("testSharedNothing", "sharednothingsingle", {})
    SesName = "TestSharedNothing"
    dsName = "testDs"
    path = "1/" + buildNativePath("csvSanity/movies.csv")

    xcalarApi = XcalarApi()
    wkbook = client.create_workbook(SesName)
    wkbook.activate()
    xcalarApi.setSession(wkbook)

    dataset = CsvDataset(xcalarApi, "testSharedNothing", path, dsName)
    dataset.load()
    dataset.delete()

    wkbook.delete()


def test_add_delete_list(client):
    # Tests adding/deleting targets, as well as listing/getting them
    name = "pytestSharedTarget"
    typ = "shared"
    params = {"mountpoint": "/netstore"}

    client.add_data_target(name, typ, params)

    targets = client.list_data_targets()

    this_targ = [t for t in targets if t.name == name][0]

    assert this_targ.type_id == "shared"
    assert this_targ.params == {
        "mountpoint": "/netstore",
        "type_context": None
    }

    this_targ = client.get_data_target(name)
    assert this_targ.name == name
    assert this_targ.type_id == typ
    assert this_targ.params == {
        "mountpoint": "/netstore",
        "type_context": None
    }

    this_targ.delete()

    targets = client.list_data_targets()

    assert len([t for t in targets if t.name == name]) == 0

    with pytest.raises(ValueError) as e:
        client.get_data_target(name)
    assert (str(e.value) == "No such target_name: '{}'".format(name))

    name_2 = name + "_2"
    name_3 = name + "_3"

    params_3 = {"mountpoint": "/"}

    dt_2 = client.add_data_target(name_2, typ, params)
    dt_3 = client.add_data_target(name_3, typ, params_3)

    assert isinstance(dt_3._to_dict(), dict)
    assert json.dumps(dt_2._to_dict()) == str(dt_2)

    dt_2.delete()
    dt_3.delete()


def test_type_list(client):
    targTypes = client._list_data_target_types()

    # Make sure we have all the target types we expect to have
    assert sorted([t["type_id"] for t in targTypes]) == sorted(targetTypes)

    # Verify that all target types have all metadata filled out correctly
    for typ in targTypes:
        assert typ["description"]
        assert typ["type_id"]
        assert typ["type_name"]
        for param in typ["parameters"]:
            assert param["name"]
            assert param["description"]
            assert "secret" in param
            assert "optional" in param

    # spot check the shared target type
    sharedType = [t for t in targTypes if t["type_id"] == "shared"][0]

    assert len(sharedType["parameters"]) == 1

    mountParam = sharedType["parameters"][0]

    assert mountParam["name"] == "mountpoint"


def test_default_targets(client):
    targets = client.list_data_targets()
    for default in defaultTargets:
        assert default in [t.name for t in targets]


def test_secret_param(client):
    targTypes = client._list_data_target_types()

    s3AccountType = [t for t in targTypes
                     if t["type_id"] == "s3fullaccount"][0]

    secretFound = False
    for param in s3AccountType["parameters"]:
        if param["name"] == "secret_access_key":
            assert param["secret"] is True
            secretFound = True
        else:
            assert param["secret"] is False
    assert secretFound


def test_error_raising(client):
    with pytest.raises(TypeError) as e:
        client.add_data_target(1, "some_type")
    assert str(e.value) == "target_name must be str, not '{}'".format(type(1))

    with pytest.raises(TypeError) as e:
        client.add_data_target("valid_target_name", 1)
    assert str(e.value) == "target_type_id must be str, not '{}'".format(
        type(1))

    with pytest.raises(TypeError) as e:
        client.add_data_target("valid_target_name", "some_type", "params")
    assert str(e.value) == "params must be dict, not '{}'".format(
        type("params"))

    incorr_type = "incorrect_type"
    with pytest.raises(ValueError) as e:
        client.add_data_target("valid_target_name", incorr_type)
    assert str(e.value) == "No such target_type_id: '{}'".format(incorr_type)

    params = {}
    incorr_param = "incorrect_param"
    params[incorr_param] = "/"
    with pytest.raises(ValueError) as e:
        client.add_data_target("valid_target_name", "shared", params)
    assert str(e.value) == "No such parameter: '{}'".format(incorr_param)

    params = {}

    with pytest.raises(ValueError) as e:
        client.add_data_target("valid_target_name", "shared", params)
    assert str(
        e.
        value) == "The following required parameters are missing: '{}'".format(
            ['mountpoint'])

    with pytest.raises(TypeError) as e:
        client.get_data_target(1)
    assert str(e.value) == "target_name must be str, not '{}'".format(type(1))

    non_exist = "non_existant_target"
    with pytest.raises(ValueError) as e:
        client.get_data_target(non_exist)
    assert str(e.value) == "No such target_name: '{}'".format(non_exist)
