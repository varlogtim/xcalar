import pytest
from pytest_bdd import scenario, given, when, then
from IPython.utils.io import capture_output

pytest.result = []

@given("I am on xcalar shell terminal")
def get_terminal(shell, capsys):
    pass

# 1
@scenario('../features/logmart_ext.feature', 'Refresh LogMart tables from cluster logs')
def test_view_config():
    # this scenario function will be executed after all of the scenario steps
    pass


@when('I enter "logmart -refresh <log_root> " in the command line')
def enter_command(shell, capsys):
    pass


@then('I should see "xxx" output')
def should_see_result(shell, capsys):
    pass


# 2
@scenario('../features/logmart_ext.feature', 'Refresh LogMart tables from specified ASUP location')
def test_logmart_asup():
    # this scenario function will be executed after all of the scenario steps
    pass

@when('I input "logmart -load_asup <asup_path> target <target_name>" in the command line')
def enter_input(shell, capsys):
    pass

@then('I can see "xxx" output')
def can_see_result(shell, capsys):
    pass


# 3
@scenario('../features/logmart_ext.feature', 'Load LogMart tables from the specified untarred loacation')
def test_load_logmart():
    # this scenario function will be executed after all of the scenario steps
    pass


@when('I type "logmart -load_logs <logs_path> target <target_name>" in the command line')
def type_command(shell, capsys):
    pass

@then('I do see "xxx" output')
def do_see_result(shell, capsys):
    pass