import datetime as dt
from pytest import fixture
from anchovies.sdk import FilesystemDatastore as Ds


@fixture
def newds(): 
    ds = Ds()
    yield ds
    try: 
        for path in ds.list_files('*'): 
            ds.delete(path)
    except Exception: 
        pass


def test_compat(): 
    try:
        assert Ds.is_compatible('./some/path')
        assert not Ds.is_compatible('s3://some/path')
    finally:
        import shutil
        from pathlib import Path
        shutil.rmtree(Path('./some'), ignore_errors=True)


def test_new_file(newds):
    path = 'example/path'
    msg = 'Hello, world!'
    with newds.open(path, 'w') as f: 
        f.write(msg)
    with newds.open(path, 'r') as f: 
        assert f.read() == msg
        

def test_read_file(newds): 
    path = 'example/other/path'
    try: 
        with newds.open(path, 'r') as f: 
            f.read()
    except FileNotFoundError: 
        pass


def test_list_files(newds): 
    for m in ['1', '2', '3']: 
        newds.write(f'example/sample{m}', m)
    files = list(newds.list_files('example/*'))
    assert files


def test_filter_files(newds):
    now = dt.datetime.now(dt.UTC)
    newds.write('newfile.txt', 'Hello, world!')
    should_be_new_file = list(newds.list_files('*', after=now))[0]
    assert should_be_new_file == 'newfile.txt'

    