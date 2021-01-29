import pytest
import re


@pytest.mark.order1
def test_materialize(shell, capsys):
    """check controller materialize"""

    shell.run_line_magic(magic_name='controller', line='-materialize')

    (out, err) = capsys.readouterr()
    messages = [rec for rec in out.split('\n')]

    steps = [
        'Previous state: materializing',
        'Persisted current state materializing',
        'Persisting current state materializing',
        'Materializing using dirty=False',
        'Previous state: streaming',
        'Previous state: snapshotting',
        'Previous state: initialized',
        'Persisting current state snapshotting',
        'Persisted current state snapshotting',
        'Persisting current state materialized',
        'Persisted current state materialized',
    ]

    assert len(messages) > 0, "No output from 'materialize'"

    for message in messages:
        assert message in steps


@pytest.mark.order2
def test_incremental_start_1(shell, capsys):
    """check incremental_start 1"""

    shell.run_line_magic(magic_name='controller', line='-incremental_start 1')

    (out, err) = capsys.readouterr()
    messages = [rec for rec in out.split('\n')]

    steps = [
        'Starting streaming with num_runs:1, sleep_between_runs:True',
        'Persisting current state materializing',
        'Previous state: streaming',
        'Previous state: materialized',
        'Previous state: initialized',
        'Persisting current state streaming',
        'Persisted current state streaming',
    ]

    assert len(messages) > 0, "No output from 'incremental_start 1'"

    for message in messages:
        assert message in steps


@pytest.mark.order3
def test_materialize_continue(shell, capsys):
    """check incremental_continue"""

    shell.run_line_magic(magic_name='controller', line='-incremental_continue')
    (out, err) = capsys.readouterr()

    assert (
        'Incremental has been unpaused. \nPlease run `controller -incremental_start <num>` again.\n\n'
        in out)


@pytest.mark.order4
def test_snapshot_take(shell, capsys):
    """controller -snapshot_take"""

    shell.run_line_magic(magic_name='controller', line='-snapshot_take')
    (out, err) = capsys.readouterr()
    messages = [rec for rec in out.split('\n')]

    steps = ['Started: Listing files', 'Started: Deleting directory var/*']
    temp = '(?:% s)' % '|'.join(steps)

    # assert "xx" in captured.stdout
    assert len(messages) > 0, "No output from 'snapshot_take'"

    for message in messages:
        # assert message in steps
        assert re.match(temp, message)


@pytest.mark.order5
def test_init(shell, capsys):

    shell.run_line_magic(magic_name='controller', line='-init')

    (out, err) = capsys.readouterr()
    messages = [rec for rec in out.split('\n')]
    steps = [
        'Initializing the controller',
    ]

    assert len(messages) == 0

    for message in messages:
        assert message in steps


@pytest.mark.order6
def test_snapshot_list(shell, capsys):

    shell.run_line_magic(magic_name='controller', line='-snapshot_list')
    (out, err) = capsys.readouterr()

    assert 'timestamp:' in out


@pytest.mark.order7
def test_snapshot_recover(shell, capsys):
    """controller snapshot_recover"""

    shell.run_line_magic(magic_name='controller', line='-snapshot_recover')
    (out, err) = capsys.readouterr()
    messages = [rec for rec in out.split('\n')]

    steps = [
        'Recovered orchestrator from snapshot',
        'Checkpointed orchestrator',
    ]

    for message in messages:
        assert message in steps
