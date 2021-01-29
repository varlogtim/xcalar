import re
from IPython.utils.io import capture_output


def test_session(shell):
    """ get session name"""
    with capture_output() as captured:
        shell.run_line_magic(magic_name='session', line='')

    assert re.match(r'\w.*', captured.stdout)
