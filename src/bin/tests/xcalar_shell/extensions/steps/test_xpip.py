from pytest_bdd import scenario, given, when, then
from IPython.utils.io import capture_output


@scenario('../features/xpip_ext.feature', 'Install a package')
def test_xpip_install():
    # this scenario function will be executed after all of the scenario steps
    pass

@given("I am on xcalar shell terminal")
def get_terminal(shell, capsys):
    pass

@when("I type xpip install ffmpeg")
def xpip_install(shell, capsys):
    shell.run_line_magic(magic_name='xpip', line='install ffmpeg')

@then("xpip list output must contains ffmpeg")
def xpip_list_contains(shell, capsys):
    with capture_output() as captured:
        shell.run_line_magic(magic_name='xpip', line='list')

    assert 'ffmpeg' in captured.stdout


@scenario('../features/xpip_ext.feature', 'Uninstall a package')
def test_xpip_uninstall():
    # this scenario function will be executed after all of the scenario steps
    pass

@when('I enter xpip uninstall "ffmpeg"')
def xpip_uninstall(shell, capsys):
    shell.run_line_magic(magic_name='xpip', line='uninstall ffmpeg')

@then('xpip list output must not contains "ffmpeg"')
def xpip_list_not_contians(shell, capsys):
    with capture_output() as captured:
        shell.run_line_magic(magic_name='xpip', line='list')

    assert 'ffmpeg' not in captured.stdout


@scenario('../features/xpip_ext.feature', 'List packages')
def test_xpip_list():
    # this scenario function will be executed after all of the scenario steps
    pass


@when("I enter xpip list")
def xpip_list(shell, capsys):
    shell.run_line_magic(magic_name='xpip', line='list')


@then("xpip list output the package list")
def show_result(shell, capsys):
    pass