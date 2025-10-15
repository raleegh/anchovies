from pytest import fixture
from anchovies.sdk import TaskStore, Task, as_task


@fixture
def newtasks(newmeta): 
    with TaskStore(newmeta): 
        yield newmeta


def test_submit(newtasks): 
    with Task() as task: 
        for i in range(10): ...
    assert task.timestamp
    task_logs = list(newtasks.db.list_files(f'{newtasks.context_home}/$task_logs/*'))
    assert task_logs


class TestException(Exception): ...


def test_error(newtasks): 
    try:
        with Task() as task: 
            raise TestException('sample')
    except TestException: 
        pass
    

def test_sequence_number(newtasks): 
    # pytest closing, so test object count
    as_task(lambda: None)()
    as_task(lambda x: None)('foo')
    task_logs = list(newtasks.db.list_files(f'{newtasks.context_home}/$task_logs/*'))
    assert len(task_logs) > 1
