from click.testing import CliRunner
import pytest

import xcalar.xc2.main
from xcalar.compute.util.cluster import DevCluster, detect_cluster

pytestmark = pytest.mark.last(
    "Execute this test as late as possible since it manages its own clusters")


# many of the features of this CLI are tested more in depth in test_cluster.py,
# which tests the underlying module
def test_cluster_health():
    detect_cluster().stop()
    # these are just the mandatory processes that will definitely be there
    processes = ["xcmonitor 0", "usrnode 0", "expServer", "xcmgmtd"]

    runner = CliRunner()

    result = runner.invoke(xcalar.xc2.main.cli, ["-vv", "cluster", "health"])
    assert result.exit_code == 1

    # all processes should be down
    assert all([p in result.output for p in processes])
    for line in result.output.splitlines()[1:]:    # skip the header
        assert "down" in line

    # start a new cluster
    with DevCluster():
        result = runner.invoke(xcalar.xc2.main.cli,
                               ["-vv", "cluster", "health"])
        assert result.exit_code == 0
        for line in result.output.splitlines()[1:]:    # skip the header
            # all mandatory processes should be up
            if any([p in line for p in processes]):
                assert "up" in line

    result = runner.invoke(xcalar.xc2.main.cli, ["-vv", "cluster", "health"])
    assert result.exit_code == 1

    # all processes should be down
    assert all([p in result.output for p in processes])
    for line in result.output.splitlines()[1:]:    # skip the header
        assert "down" in line


def test_cluster_start_stop():
    runner = CliRunner()

    assert detect_cluster().is_fully_down()

    result = runner.invoke(xcalar.xc2.main.cli, ["-vv", "cluster", "start"])
    assert result.exit_code == 0

    assert detect_cluster().is_up()

    result = runner.invoke(xcalar.xc2.main.cli, ["-vv", "cluster", "stop"])
    assert result.exit_code == 0

    assert detect_cluster().is_fully_down()
