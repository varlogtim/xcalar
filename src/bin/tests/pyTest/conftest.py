from xcalar.compute.util.cluster import DevCluster


# This is disabled because the way we are handling setup_class is incorrect
# @pytest.fixture(scope="session", autouse=True)
def xceCluster():
    with DevCluster() as cluster:
        yield cluster
