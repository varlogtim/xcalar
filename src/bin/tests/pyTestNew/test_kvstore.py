import pytest

from xcalar.external.client import Client
from xcalar.external.exceptions import XDPException
from xcalar.external.kvstore import KvStore as KvStore

from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.LibApisCommon.ttypes import XcalarApiWorkbookScopeT


@pytest.fixture(scope="module")
def session(workbook):
    session = workbook.activate()
    yield session
    session.destroy()


@pytest.fixture(scope="module")
def kvstore(client):
    global_store = client.global_kvstore()
    yield global_store


@pytest.fixture(scope="module")
def session_kvstore(session):
    session_store = KvStore.workbook_by_name(session.client, session.username, session.name)
    yield session_store


def test_basic(kvstore):
    key = "TestBasicKey1234"
    value = "hello world kvstore value!"
    client = Client()
    kvstore = client.global_kvstore()

    for persist in [True, False]:
        # set the key
        kvstore.add_or_replace(key, value, persist=persist)

        # check the key
        assert kvstore.lookup(key) == value

        # remove the key
        kvstore.delete(key)

        assert key not in kvstore.list()


def test_sanity(kvstore):
    key = "KVSimpleTestKey"

    # Clean up key if it exists.
    try:
        kvstore.delete(key)
    except XDPException as e:
        assert e.statusCode == StatusT.StatusKvEntryNotFound

    # Ensure key is deleted
    with pytest.raises(XDPException) as exc:
        kvstore.lookup(key)
    assert exc.value.statusCode == StatusT.StatusKvEntryNotFound

    assert key not in kvstore.list()

    kvstore.add_or_replace(key, "value1", True)

    assert key in kvstore.list()
    assert key in kvstore.list(key)
    assert key in kvstore.list(f"{key}|other")
    assert key not in kvstore.list("falsepattern")

    assert kvstore.lookup(key) == "value1"

    kvstore.append(key, "2")

    assert kvstore.lookup(key) == "value12"

    with pytest.raises(XDPException) as exc:
        kvstore.set_if_equal(True, key, "valueFalse", "valueTrue")
    assert exc.value.statusCode == StatusT.StatusKvEntryNotEqual
    assert key in kvstore.list()

    with pytest.raises(XDPException) as exc:
        kvstore.set_if_equal(True, key + "Unmatch", "value12", "valueTrue")
    assert exc.value.statusCode == StatusT.StatusKvEntryNotFound

    kvstore.set_if_equal(True, key, "value12", "value3")

    assert kvstore.lookup(key) == "value3"

    # TODO: check keySecondary/valueSecondary functionality

    kvstore.delete(key)

    assert key not in kvstore.list()

    with pytest.raises(XDPException) as exc:
        kvstore.lookup(key)
    assert exc.value.statusCode == StatusT.StatusKvEntryNotFound

    with pytest.raises(XDPException) as exc:
        kvstore.delete(key)
    assert exc.value.statusCode == StatusT.StatusKvEntryNotFound


def test_invalid_regex(kvstore):
    with pytest.raises(XDPException) as exc:
        # invalid regex
        kvstore.list(".(*")
    assert exc.value.statusCode == StatusT.StatusFailed


def test_multi_update(session_kvstore):
    key1, key2 = "foo", "bar"
    value1, value2 = "foo_value", "bar_value"
    session_kvstore.add_or_replace(key1, value1, True)
    session_kvstore.add_or_replace(key2, value2, True)
    assert key1 in session_kvstore.list()
    assert key2 in session_kvstore.list()
    assert session_kvstore.lookup(key1) == value1
    assert session_kvstore.lookup(key2) == value2

    value1, value2 = "foo_value_updated", "bar_value_updated"
    kv_dict = {key1: value1, key2: value2}
    session_kvstore.multi_add_or_replace(kv_dict, True)
    assert key1 in session_kvstore.list()
    assert key2 in session_kvstore.list()
    assert session_kvstore.lookup(key1) == value1
    assert session_kvstore.lookup(key2) == value2
