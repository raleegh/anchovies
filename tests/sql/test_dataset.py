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
    # creator = db.engine.pool._creator
    # db.engine.pool = QueuePool(creator, pool_size=1, max_overflow=1)
    # yield db


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


# def test_16_connections(dataset): 
#     '''Base dataset has a bad piece of code where you 
#     essentially can't use more than 15 connections.
#     '''
#     def connect_and_sleep(*args): 
#         with dataset: 
#             sleep(2)
#     with cf.ThreadPoolExecutor(2) as exe: 
#         futs = tuple(exe.map(connect_and_sleep, range(2)))
#     for fut in cf.as_completed(futs):
#         fut.result()
## can't figure out how to test this...
### base dataset also fails on this but not because of the queue pool
### sql alchemy uses a SingletonPoolExecutor which doesn't work as expected???