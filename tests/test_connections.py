from pytest import fixture


@fixture
def cnxns(downloader):
    downloader.connections.find()
    yield downloader.connections


def test_find(downloader): 
    downloader.connections.find()
    assert hasattr(downloader, 'test_cnxn')
    assert hasattr(downloader.test_cnxn, 'arg1')


def test_connect(downloader, cnxns): 
    with cnxns: 
        assert downloader.test_cnxn.is_connected


def test_disonnected(downloader, cnxns): 
    with cnxns: 
        assert not downloader.test_no_connect.is_connected


def test_different_id(downloader): 
    downloader.connections.find()
    assert hasattr(downloader, 'weird_cnxn')
    assert hasattr(downloader.weird_cnxn, 'arg1')


def test_json_data(downloader): 
    downloader.connections.find()
    assert hasattr(downloader, 'json_cnxn')
    assert hasattr(downloader.json_cnxn, 'arg1')
