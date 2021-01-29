import pytest
from pytest_bdd import scenario, given, when, then
from IPython.utils.io import capture_output

pytest.result = []


@given("I am on xcalar shell terminal")
def get_terminal(shell, capsys):
    pass


@scenario('../features/config_editor_ext.feature', 'view config file')
def test_view_config():
    # this scenario function will be executed after all of the scenario steps
    pass


@when(u'I enter "%%config_editor -v /etc/xcalar/default.cfg" in the command line')
def enter_view_command(shell, capsys):
    pass
    # with capture_output() as captured:
    #     shell.run_cell_magic(magic_name=u'config_editor', line=u'', cell='-v /etc/xcalar/default.cfg\n\n')
    #     pytest.result = captured

    # print(pytest.result.stdout)


@then('I should be able to see "Thrift.Port=9090" in the output')
def see_output(shell, capsys):
    pass
    # assert 'Thrift.Port=9090' in pytest.result.stdout






@scenario('../features/config_editor_ext.feature', 'edit config file')
def test_edit_config():
    # this scenario function will be executed after all of the scenario steps
    pass


@given('I type "%%config_editor -e /etc/xcalar/default.cfg" in the command line')
def type_command(shell, capsys):
    pass


@when('I type "Node.0ApiPort=18555" in the next line')
def input_content(shell, capsys):
    pass

@then('"Node.0ApiPort=18555" existing in the "/etc/xcalar/default.cfg"')
def see_result(shell, capsys):
    pass