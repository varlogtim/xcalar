from pytest_bdd import scenario, given, when, then
from IPython.utils.io import capture_output


@given("I am on xcalar shell terminal")
def get_terminal(shell, capsys):
    pass


# 1
@scenario('../features/systemstat_ext.feature', 'check cpu stats')
def test_cpu_stats(shell, capsys):
    # this scenario function will be executed after all of the scenario steps
    pass


@when('I enter "systemstat -n 0 -mpstat" in the command line')
def mpstat(shell, capsys):
    pass


@then('I would see "cpu stats" output')
def would_see_result(shell, capsys):
    pass


# 2
@scenario('../features/systemstat_ext.feature', 'check memory stats')
def test_memory_stats(shell, capsys):
    # this scenario function will be executed after all of the scenario steps
    pass


@when('I enter "systemstat -n 0 -vmstat" in the command line')
def vmstat(shell, capsys):
    pass

@then('I should see "memory stats" output')
def should_see_result(shell, capsys):
    pass

# 3
@scenario('../features/systemstat_ext.feature', 'system-cgls')
def test_system_cgls(shell, capsys):
    # this scenario function will be executed after all of the scenario steps
    pass



@when('I enter "systemstat -n 0 -systemd-cgls" in the command line')
def systemd_cgls(shell, capsys):
    pass

@then('I could see "systemd-cgls" output')
def could_see_result(shell, capsys):
    pass


# 4
@scenario('../features/systemstat_ext.feature', 'cgroup Controller until')
def test_cgroupControllerUtil(shell, capsys):
    # this scenario function will be executed after all of the scenario steps
    pass


@when('I enter "systemstat -n 0 -cgroupControllerUtil <options>" in the command line')
def cgroupControllerUtil(shell, capsys):
    pass



@then('I do see "cgroupControllerUtil" output')
def do_see_result(shell, capsys):
    pass

