from pytest_bdd import scenario, given, when, then
from IPython.utils.io import capture_output

@given("I am on xcalar shell terminal")
def get_terminal(shell, capsys):
    pass


# 1
@scenario('../features/systemctl_ext.feature', 'systemctl reload <service>')
def test_reload_service(shell, capsys):
    # this scenario function will be executed after all of the scenario steps
    pass


@when('I enter "systemctl reload xcalar-usrnode" in the command line')
def reload_service():
    pass

@then('I would see "xxx"')
def would_see(shell, capsys):
    pass


# 2
@scenario('../features/systemctl_ext.feature', 'systemctl restart <service>')
def test_restart_service(shell, capsys):
    # this scenario function will be executed after all of the scenario steps
    pass

@when('I enter "systemctl restart xcalar-usrnode" in the command line')
def restart_service():
    pass

@then('I should see "xxx"')
def should_see(shell, capsys):
    pass


# 3
@scenario('../features/systemctl_ext.feature', 'systemctl start <service>')
def test_start_service(shell, capsys):
    # this scenario function will be executed after all of the scenario steps
    pass

@when('I enter "systemctl start xcalar-usrnode" in the command line')
def start_service():
    pass

@then('I could see "xxx"')
def could_see(shell, capsys):
    pass

# 4
@scenario('../features/systemctl_ext.feature', 'systemctl stop <service>')
def test_stop_service(shell, capsys):
    # this scenario function will be executed after all of the scenario steps
    pass

@when('I enter "systemctl stop xcalar-usrnode" in the command line')
def stop_service():
    pass

@then('I might see "xxx"')
def might_see(shell, capsys):
    pass


# 5
@scenario('../features/systemctl_ext.feature', 'systemctl status <service>')
def test_status_service(shell, capsys):
    # this scenario function will be executed after all of the scenario steps
    pass

@when('I enter "systemctl status xcalar-usrnode" in the command line')
def status_service():
    pass

@then('I do see "xxx"')
def do_see(shell, capsys):
    pass


# 6
@scenario('../features/systemctl_ext.feature', 'systemctl list-units <service>')
def test_list_units(shell, capsys):
    # this scenario function will be executed after all of the scenario steps
    pass

@when('I enter "systemctl list-units" in the command line')
def list_units():
    pass

@then('I did see "xxx"')
def did_see(shell, capsys):
    pass