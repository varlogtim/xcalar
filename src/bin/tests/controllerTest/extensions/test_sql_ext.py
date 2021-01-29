from IPython.utils.io import capture_output


def test_sql(shell, capsys):

    shell.run_line_magic(magic_name='controller', line='-materialize')

    with capture_output() as captured:
        shell.run_line_magic(magic_name='sql', line='select count(*) from ds1')

    assert 'COUNT_1' in captured.stdout

    assert (
        '\x1b[2mWarning: \x1b[0m 500 Server Error: Cannot find all published tables for url: https://xdp-mtang-132.westus2.cloudapp.azure.com/app/service/xce\n'
        not in captured.stdout)
