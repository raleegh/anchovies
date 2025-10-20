import logging
from pytest import fixture
from anchovies.plugins.core.dataset import Dataset, TablePlus, ScdTable
logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)


@fixture
def db_uri(): 
    return 'sqlite:///:memory:'


@fixture
def dataset(db_uri): 
    yield Dataset(db_uri)


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
