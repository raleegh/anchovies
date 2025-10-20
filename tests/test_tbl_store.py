from pytest import fixture
from anchovies.sdk import Downloader, Session, Datastore, TblStore


@fixture
def tblstore(newmeta): 
    with TblStore(newmeta) as tbls:
        yield tbls


@fixture
def setup_multiple(newmeta, config_str): 
    with Session(Downloader, anchovy_id='anchovy1', config_str=config_str) as ses, \
            Datastore.new():
        with TblStore(newmeta) as tbls:
            pass
    with Session(Downloader, anchovy_id='anchovy2', config_str=config_str) as ses, \
            Datastore.new():
        with TblStore(newmeta) as tbls:
            pass
    with Session(
        Downloader, 
        anchovy_id='anchovy3',
        upstream=('anchovy1', 'anchovy2'),
    ) as ses, \
            Datastore.new() as meta:
        yield meta


def test_config(session): 
    assert isinstance(session.config_yaml, dict)

 
def test_read(session, tblstore): 
    assert list(tblstore.tbls)


def test_save(tblstore, newmeta): 
    tblstore.flush()
    db = newmeta
    files = list(db.list_files(newmeta.anchovy_home('$tables', '*')))
    assert files


def test_downstream_read(setup_multiple): 
    '''Test that a "downstream" anchovy can read files from "upstream anchovy.'''
    meta = setup_multiple
    with TblStore(meta) as tbls: 
        tbls = list(tbls)
    assert len(tbls) >= 4, 'There should be 4 tbls generated.'