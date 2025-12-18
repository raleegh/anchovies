from anchovies.sdk.tasks import Task


def test_start():
    with Task(lambda: list(range(1_000_000))):
        ...


def test_callback(): 
    stopped = [False]
    def set_stop():
        stopped[0] = True
    task = Task(lambda: list(range(1_000_000)))
    task.on_stop(set_stop)
    with task: 
        ...
    assert stopped[0]


def test_overseer(): 
    from anchovies.sdk.overseers import overseer
    with Task(lambda: list(range(1_000_000))):
        assert overseer().tasks
        

def test_nesting_inner_failure(): 
    def always_raise(): 
        raise Exception()
    task1 = Task(lambda: list(range(100_000_000)))
    task2 = Task(always_raise)
    with task1: 
        task1.also_promise(task2)
    assert not task1.crashed.is_set()
    assert task2.crashed.is_set()


def test_nesting_outer_failure(): 
    def always_raise(): 
        raise Exception()
    task1 = Task(lambda: list(range(100_000_000)))
    task2 = Task(always_raise)
    with task2: 
        task2.also_promise(task1)
  