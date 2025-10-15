from pytest import fixture
from anchovies.sdk import Anchovy, Downloader, SessionResult, source, sink


class TestDownloader(Downloader): 
    @source()
    def generate(self, **kwds): 
        yield {'id': 1}

    @sink()
    def default_sink(self, **kwds): 
        pass


class BrokenDownloader(Downloader): 
    @source('sales')
    def get_bad_table(self, **kwds): 
        raise RuntimeError()
    
    @source('contacts')
    def get_contacts(self, **kwds): 
        yield {'id': 1}

    @sink()
    def default_sink(self, **kwds): 
        pass
  

@fixture
def anchovy(): 
    a = Anchovy('anchovy_test', 'test')
    yield a


def test_anchovy_run(anchovy, config_str):
    results = anchovy.run(TestDownloader, config_str=config_str) 
    assert isinstance(results, SessionResult)
    assert results.status == 'OK'
    assert results.startup_at
    assert results.shutdown_at
    assert results.session_id
    assert results.batches
    assert results.startup_task
    assert results.shutdown_task

    batch = results.batches[0]
    assert batch['batch_id']
    assert batch['status']
    assert batch['opened_at']
    assert batch['closed_at']
    assert batch['done_tasks']

    task = batch['done_tasks'][2]
    assert task['task_type']
    assert task['thread_id']
    assert task['status']
    assert task['timestamp']
    assert task['operator']

    task_types = set(task['task_type'] for task in batch['done_tasks'])
    assert 'XOPEN' in task_types
    assert 'TABLE' in task_types
    assert 'XCLOSE' in task_types


def test_failed_anchovy_run(anchovy, config_str): 
    results = anchovy.run(BrokenDownloader, config_str=config_str)
    # TODO: write class for "breaking" downloader
    assert results.status == 'ERR'

    batch = results.batches[0]
    assert batch['status'] == 'ERR'

    task = results.batches[0]['done_tasks'][2]
    assert task['status'] == 'ERR'
