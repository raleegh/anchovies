import datetime as dt
from pytest import fixture
from anchovies.sdk import Datastore, Checkpoint, CachedCheckpoint


@fixture
def newckpoint(newmeta: Datastore): 
    with Checkpoint(newmeta) as ck: 
        yield ck


@fixture
def newcachedckpoint(newmeta: Datastore): 
    with CachedCheckpoint(newmeta) as ck: 
        yield ck

def test_get_put(newckpoint): 
    key = 'some.checkpoint'
    msg = 'Hello!'
    newckpoint.put(key, msg)
    assert newckpoint.get(key) == msg


def test_chached(newmeta): 
    ck = CachedCheckpoint(newmeta)
    with ck: 
        ck.put('some.checkpoint', 'Hello!')
        ck.put('some.other.checkpoint', 'Hello again!')

    with ck: 
        assert ck.data
        ck.put('some.checkpoint', 'update')
        assert ck['some', 'checkpoint'] == 'update'


def test_hint(newckpoint): 
    newckpoint['test'] = '2025-01-01 00:00:00'
    newckpoint.hint('test', des=dt.datetime.fromisoformat, ser=dt.datetime.isoformat)
    should_be_datetime = newckpoint['test']
    assert isinstance(should_be_datetime, dt.datetime)


def test_wildcard_hint(newckpoint): 
    newckpoint['test.last.datetime'] = '2025-01-01 00:00:00'
    newckpoint.hint('*.datetime', des=dt.datetime.fromisoformat, ser=dt.datetime.isoformat)
    should_be_datetime = newckpoint['test.last.datetime']
    assert isinstance(should_be_datetime, dt.datetime)


def test_cached_checkpoint(newcachedckpoint): 
    ck = newcachedckpoint
    ck['test.last.datetime'] = '2025-01-01 00:00:00'
    ck.hint('*.datetime', des=dt.datetime.fromisoformat, ser=dt.datetime.isoformat)
    should_be_datetime = ck['test.last.datetime']
    assert isinstance(should_be_datetime, dt.datetime)
    assert ck.data
    ck.close()
    assert not ck._changed