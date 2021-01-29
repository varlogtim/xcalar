from pytest_bdd import scenario, given, when, then
from IPython.utils.io import capture_output


@given("I am on xcalar shell terminal")
def get_terminal(shell, capsys):
    pass


# 1
@scenario('../features/monitor_ext.feature', 'monitor cpu stats')
def test_cpu_stats():
    # this scenario function will be executed after all of the scenario steps
    pass


@when('I enter "monitor -n 0 mpstat" in the command line')
def enter(shell, capsys):
    pass


@then('I would see "xxx"')
def would_see(shell, capsys):
    pass

# 2
@scenario('../features/monitor_ext.feature', 'monitor memory stats')
def test_memory_stats():
    # this scenario function will be executed after all of the scenario steps
    pass

@when('I enter "monitor -n 0 vmstat" in the command line')
def I_enter(shell, capsys):
    pass


@then('I have seen "xxx"')
def have_seen(shell, capsys):
    pass

# 3
@scenario('../features/monitor_ext.feature', 'systemd-cgls')
def test_systemd_cgls():
    # this scenario function will be executed after all of the scenario steps
    pass

@when('I input "monitor -n -systemd-cgls" in the command line')
def I_input(shell, capsys):
    pass


@then('I could see "xxx"')
def could_see_result(shell, capsys):
    pass

# 4
@scenario('../features/monitor_ext.feature', 'view cluster nodes')
def test_view_cluster_nodes():
    # this scenario function will be executed after all of the scenario steps
    pass

@when('I enter "monitor -nodes" in the command line')
def enter_monitor_node(shell, capsys):
    pass


@then('I should see "node\'s IP address"')
def should_see_result(shell, capsys):
    pass