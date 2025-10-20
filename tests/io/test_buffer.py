from pytest import fixture
from datetime import datetime
from anchovies.plugins.core.io import DefaultDataBuffer


@fixture
def buffer(newmeta, session): 
    yield DefaultDataBuffer('some_table', 'test_io')


def test_write(buffer): 
    with buffer:
        buffer.write({'id': 1})


def test_write_typing(buffer): 
    with buffer: 
        pos = buffer.tell()
        buffer.write({'dt': datetime.now()})
    with buffer: 
        buffer.seek(pos)
        rows = tuple(buffer.read())
    assert isinstance(rows[0]['dt'], datetime)

