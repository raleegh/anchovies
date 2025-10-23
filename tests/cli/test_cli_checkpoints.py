from pytest import fixture
from click.testing import CliRunner
from anchovies.cli import cli


@fixture
def runner(): 
    yield CliRunner()


def test_list_checkpoints(runner):
    resp = runner.invoke(cli, ('checkpoints', '--id', 'test', '--user', 'test'))
    assert 'None found!' not in resp.stdout


def test_delete_checkpoint(runner): 
    resp = runner.invoke(cli, ('checkpoints', '--id', 'test', '--user', 'test', '-D', '-y', 'some.checkpoint'))
    ...
    