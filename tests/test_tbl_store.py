from pytest import fixture
from anchovies.sdk import Downloader, Session, Metastore, TblStore, ID


@fixture
def tblstore(newmeta): 
    with TblStore(newmeta) as tbls:
        yield tbls


@fixture
def setup_multiple(newmeta, config_str): 
    with Session(Downloader, anchovy_id='anchovy1', config_str=config_str) as ses, \
            Metastore():
        with TblStore(newmeta) as tbls:
            pass
    with Session(Downloader, anchovy_id='anchovy2', config_str=config_str) as ses, \
            Metastore():
        with TblStore(newmeta) as tbls:
            pass
    with Session(
        Downloader, 
        anchovy_id='anchovy3',
        upstream=('anchovy1', 'anchovy2'),
    ) as ses, \
            Metastore() as meta:
        yield meta


def test_config(session): 
    assert isinstance(session.config_yaml, dict)

 
def test_read(session, tblstore): 
    assert list(tblstore.tbls)


def test_save(tblstore, newmeta): 
    tblstore.flush()
    db = newmeta.db
    files = list(db.list_files(newmeta.context_home + '/$tables/*'))
    assert files


def test_downstream_read(setup_multiple): 
    '''Test that a "downstream" anchovy can read files from "upstream anchovy.'''
    meta = setup_multiple
    with TblStore(meta) as tbls: 
        assert list(tbls)
