import logging
import concurrent.futures as cf
from time import sleep
from pytest import fixture
from sqlalchemy.pool import QueuePool
from anchovies.plugins.core.dataset import Dataset, TablePlus, ScdTable
logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)


@fixture
def db_uri(): 
    return 'sqlite:///:memory:?check_same_thread=False'


@fixture
def dataset(db_uri): 
    yield Dataset(db_uri)


@fixture
def dataset_w_limited_pool(db_uri): 
    # yield Dataset(db_uri)
    db = Dataset(db_uri)
    creator = db.engine.pool._creator
    db.engine.pool = QueuePool(
        creator, 
        pool_size=1, 
        max_overflow=0, 
        timeout=1,
    )
    yield db


def test_insert_many(dataset): 
    rows = [
        {'id': 1, 'name': 'hello'}, 
        {'id': 2, 'name': 'world'},
        {'id': 3, 'name': 'third record!'},
    ]
    dataset.table_cls = TablePlus
    tbl = dataset['test']
    with dataset: # test that transaction behavior working as expected
        inserted = tbl.insert_many(rows)
    assert inserted
    assert isinstance(tbl, TablePlus)
    assert tbl.count() == 3
    sample = tbl.find_one(id=3)
    assert sample
    assert sample['name'] == rows[2]['name']


def test_scd_insert_many(dataset): 
    rows = [
        {'guid': 'ABC', 'price': 3.0, 'product_id': 'pr2'},
        {'guid': 'DEF', 'price': 4.0, 'product_id': 'pr2'},
        {'guid': 'GHI', 'price': 5.0, 'product_id': 'pr2'},
    ]
    dataset.table_cls = ScdTable
    tbl = dataset.create_table('test_scd', 'guid', 'str')
    with dataset: 
        inserted = tbl.insert_many(rows)
    with dataset: 
        inserted = tbl.insert_many(rows)
    assert isinstance(tbl, ScdTable)
    assert tbl.count() == 6
    assert tbl.count(_del=None) == 3


def test_connection_released_back_to_pool(dataset_w_limited_pool):
    '''Check that dataset releases connections back to the QueuePool.
    
    There is a problem with the basic dataset implementation where 
    it doesn't send connections back to the SQL Alchemy Pool after use.
    This means that after 15 connections, an error is raised.
    '''
    dataset = dataset_w_limited_pool
    for _ in range(2): 
        exe = dataset.executable
        exe.execute('select 1')



def test_connection_released_back_to_pool_begin_commit(dataset_w_limited_pool):
    '''Check that dataset releases connections back to the QueuePool.
    
    There is a problem with the basic dataset implementation where 
    it doesn't send connections back to the SQL Alchemy Pool after use.
    This means that after 15 connections, an error is raised.

    This test works against the begin/commit interface.
    '''
    dataset = dataset_w_limited_pool
    for _ in range(2): 
        with dataset: 
            exe = dataset.executable
            exe.execute('select 1')
    