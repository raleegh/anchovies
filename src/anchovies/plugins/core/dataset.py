import os
import io
import inspect
import itertools
import sqlalchemy.types
import uuid
import logging
from abc import ABC
from datetime import date, datetime
from decimal import Decimal
from typing import cast, Any, Sequence, Callable

# ensure typing is linter friendly
class Database: ...
class Table: ...
class OriginalTypes: ...
try:
    from dataset import Database, Table
    from dataset.types import Types as OriginalTypes
    from dataset.table import DatasetException, SQLATable, Column
    from sqlalchemy import select, func
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine import Connection as SqlConnection
    from sqlalchemy.orm import Session
    String = sqlalchemy.types.Unicode
    Binary = sqlalchemy.types.LargeBinary
except ImportError: 
    pass

from anchovies.sdk import Connection


logger = logging.getLogger('anchovies.dataset')
PRIMARY_LENGTH = int(os.getenv('DATASET_PRIMARY_LENGTH', 40))
DEFAULT_LENGTH = int(os.getenv('DATASET_DEFAULT_LENGTH', 255))
CHUNKSIZE = int(os.getenv('DATASET_CHUNKSIZE', io.DEFAULT_BUFFER_SIZE * 10))
SAMPLE_SIZE = os.getenv('DATASET_SAMPLE_SIZE', None)
SAMPLE_SIZE = int(SAMPLE_SIZE) if SAMPLE_SIZE else None
DELETED_TTL = os.getenv('DATASET_DELETED_TTL', None)
DELETED_TTL = int(DELETED_TTL) if DELETED_TTL else None
DataStream = Sequence[dict[str, Any]]


class Dataset(Database, Connection): 
    '''A SQL Dataset.'''
    engine: Engine

    def __init__(
        self, 
        url: str, 
        schema: str=None, 
        engine_kwargs=None, 
        ensure_schema=True, 
        row_type=dict, 
        sqlite_wal_mode=True, 
        on_connect_statements=None,
    ):
        super().__init__(url, schema, engine_kwargs, ensure_schema, row_type, sqlite_wal_mode, on_connect_statements)
        self.types = TypesPlus(is_postgres=self.is_postgres)
        self.table_cls = ScdTable
        self._context_level = 0

    def __getitem__(self, table_name) -> 'TablePlus':
        return super().__getitem__(table_name)
    
    def get_executable(self): 
        '''Get (contextually) a SQLAlchemy Executable for the underlying engine.'''
        if hasattr(self.local, 'executable'): 
            return cast(SqlConnection, self.local.executable)
        return self.engine.connect()
    
    def set_executable(self): 
        '''Add (if not set) a SQLAlchemy Executable to thread's local storage.'''
        self._context_level += 1
        if hasattr(self.local, 'executable'): 
            return
        self.local.executable = self.engine.connect()

    def unset_executable(self): 
        '''Remove the SQL Alchemy executable from the thread's local storage.'''
        self._context_level -= 1
        if self._context_level == 0: 
            if hasattr(self.local, 'executable'): 
                del self.local.executable

    @property
    def executable(self): 
        return self.get_executable()

    def begin(self):
        self.set_executable()
        return super().begin()

    def commit(self):
        super().commit()
        self.unset_executable()

    def rollback(self):
        super().rollback()
        self.unset_executable()
    
    def create_table(self, table_name, primary_id=None, primary_type=None, primary_increment=None, **settings) -> 'TablePlus':
        """Create a new table.

        Either loads a table or creates it if it doesn't exist yet. You can
        define the name and type of the primary key field, if a new table is to
        be created. The default is to create an auto-incrementing integer,
        ``id``. You can also set the primary key to be a string or big integer.
        The caller will be responsible for the uniqueness of ``primary_id`` if
        it is defined as a text type. You can disable auto-increment behaviour
        for numeric primary keys by setting `primary_increment` to `False`.

        Returns a :py:class:`Table <dataset.Table>` instance.
        ::

            table = db.create_table('population')

            # custom id and type
            table2 = db.create_table('population2', 'age')
            table3 = db.create_table('population3',
                                     primary_id='city',
                                     primary_type=db.types.text)
            # custom length of String
            table4 = db.create_table('population4',
                                     primary_id='city',
                                     primary_type=db.types.string(25))
            # no primary key
            table5 = db.create_table('population5',
                                     primary_id=False)
        """
        with self.lock:
            if table_name not in self._tables:
                self._tables[table_name] = self.table_cls(
                    self,
                    table_name,
                    primary_id=primary_id,
                    primary_type=primary_type,
                    primary_increment=primary_increment,
                    auto_create=True,
                    **settings,
                )
            return self._tables.get(table_name)
        
    def create_temp_table(self, primary_id=None, primary_type=None, primary_increment=None, types=None) -> 'TablePlus': 
        name = '_tmp' + str(uuid.uuid4()).replace('-', '')[:12]
        tbl = self.create_table(name, primary_id, primary_type, primary_increment)
        if types: 
            tbl.sync_columns(types)
        return tbl

    def watch_primary_type(self, primary_id: str | tuple, stream: DataStream) -> tuple[tuple, DataStream]: 
        '''Observe the primary types of a table based on 1 row.'''
        logger.debug(f'watch the primary type for ids {primary_id}...')
        for row in stream: 
            break
        types = list()
        for col in primary_id: 
            val = row.get(col) 
            if val is None: 
                raise ValueError(f'A null value was received for {col} in {self} on row {row}.')
            t = self.types.guess(val)
            types.append(t)
        types = tuple(types)
        logger.debug(f'discovered {primary_id} as {types}...')
        return types, itertools.chain([row], stream)


class TablePlus(Table): 
    '''
    An enhanced version of Dataset's `Table`.

    Now supporting: 
    * string lengths
    * multi-column primary keys
    * better upserts
    '''
    db: Dataset 

    def __init__(
        self, 
        database: Dataset, 
        table_name: str, 
        primary_id: Sequence | str=None, 
        primary_type: Sequence | str=None, 
        primary_increment: bool=None, 
        auto_create=False, 
        primary_length: int=PRIMARY_LENGTH,
        default_length: int=DEFAULT_LENGTH,
        sort_by: dict=None,
        **settings,
    ):
        super().__init__(database, table_name, primary_id, primary_type, primary_increment, auto_create)
        if self._primary_type is OriginalTypes.integer: 
            self._primary_type = self.db.types.bigint
        if isinstance(self._primary_id, str): 
            self._primary_id = (self._primary_id,)
            self._primary_type = tuple(self._primary_type for _ in range(len(self._primary_id)))
        self._primary_length = primary_length
        self._default_length = default_length
        self._sort_by = sort_by
        # TODO: ensure that in length based databases default length works

    def __enter__(self): 
        return self 
    
    def __exit__(self, *args): 
        self.drop()

    def sync_table(self, columns):
        """Lazy load, create or adapt the table structure in the database."""
        if self._table is None:
            # Load an existing table from the database.
            self._reflect_table()
        if self._table is None:
            # Create the table with an initial set of columns.
            if not self._auto_create:
                raise DatasetException("Table does not exist: %s" % self.name)
            # Keep the lock scope small because this is run very often.
            with self.db.lock:
                self._threading_warn()
                self._table = SQLATable(
                    self.name, self.db.metadata, schema=self.db.schema
                )
                for column in self.meta_iter_primary_columns():
                    self._table.append_column(column)
                for column in columns:
                    if not column.name in self._primary_id:
                        self._table.append_column(column)
                self._table.create(self.db.executable, checkfirst=True)
                self._columns = None
        elif len(columns):
            with self.db.lock:
                self._reflect_table()
                self._threading_warn()
                for column in columns:
                    if not self.has_column(column.name):
                        self.db.op.add_column(self.name, column, schema=self.db.schema)
                self._reflect_table()
    _sync_table = sync_table

    def meta_iter_primary_columns(self): 
        '''Yields all columns defined as "primary".'''
        for col, dtype in zip(self._primary_id, self._primary_type): 
            yield Column(
                col, 
                self.meta_make_dtype(dtype, self._primary_length), 
                primary_key=True, 
                autoincrement=self._primary_increment,
            )
    
    def meta_make_dtype(self, dtype_name: str, length: int=0): 
        '''Constructs a SQL Alchemy data type, 
        even if the inputs are incompatible.'''
        dtype = self.db.types.guess(dtype_name)
        if self.db.types.uses_length(dtype): 
            return dtype(length)
        return dtype
    
    def watch_columns(self, stream: DataStream, *, sample_size: int=SAMPLE_SIZE) -> tuple[dict, DataStream]: 
        sync_row = {}
        watched = list()
        stream = iter(stream)
        for i, row in enumerate(stream):
            # Only get non-existing columns.
            sync_keys = list(sync_row.keys())
            for key in [k for k in row.keys() if k not in sync_keys]:
                # Get a sample of the new column(s) from the row.
                sync_row[key] = row[key]
            watched.append(row)
            if sample_size and i > sample_size: 
                break
        return sync_row, itertools.chain(watched, stream)

    def sync_columns(self, row, ensure=True, types=None):
        """Create missing columns (or the table) prior to writes.

        If automatic schema generation is disabled (``ensure`` is ``False``),
        this will remove any keys from the ``row`` for which there is no
        matching column.
        """
        ensure = self._check_ensure(ensure)
        types = types or {}
        types = {self._get_column_name(k): v for (k, v) in types.items()}
        out = {}
        sync_columns = {}
        for name, value in row.items():
            name = self._get_column_name(name)
            if self.has_column(name):
                out[name] = value
            elif ensure:
                _type = types.get(name)
                if _type is None:
                    _type = self.db.types.guess(value)
                    if _type is None: 
                        continue
                if isinstance(_type, str): 
                    _type = self.db.types.guess(_type)
                sync_columns[name] = Column(name, _type)
                out[name] = value
        self.sync_table(sync_columns.values())
        return out
    _sync_columns = sync_columns

    def insert_many(self, rows, chunk_size=CHUNKSIZE, sample_size=SAMPLE_SIZE, setup_hook: Callable=0, **kwds) -> int:
        sample_size = sample_size if CHUNKSIZE == -1 else None
        rows_inserted = 0
        for chunk in chunked_with_ceiling(rows, chunk_size): 
            # Get columns name list to be used for padding later.
            columns, chunk = self.watch_columns(chunk, sample_size=sample_size)
            if setup_hook is not None:
                if setup_hook == 0: 
                    setup_hook = type(self).sync_columns
                setup_hook(self, columns, **kwds)
            try: 
                rows_inserted += self.insert_bulk(chunk, columns)
                continue
            except NotImplementedError: 
                logger.warning(f'{type(self)}.insert_bulk() not implemented -> use sqlalchemy')
                pass
            logger.debug('Using SQL Alchemy to write to db')
            chunk = self.cleanup(chunk, columns)
            with Session(self.db.executable) as ses, ses.begin(): 
                chunk = tuple(chunk)
                rows_inserted += len(chunk)
                logger.debug(f'Attempt to insert {len(chunk):,} rows')
                ses.execute(self.table.insert(), chunk)
        return rows_inserted
            
    def insert_bulk(self, stream: DataStream, columns: dict=None) -> int: 
        raise NotImplementedError(f'This must be provided')
    
    def cleanup(self, stream: DataStream, columns: dict) -> DataStream: 
        stream = map(enforce_columns(columns), stream)
        return stream 
    

class ScdTable(TablePlus): 
    '''A table that can run simplified SCD-style inserts.
    
    An SCD is a "slowly changing dimension". This means that the table 
    keeps a change-log of the data, rather than updating matching rows.

    Two columns will be added to your table: 
    * `_seq` -> the relative time in microseconds that the record is added
    * `_del` -> the relative time in microseconds that the record is 
        deleted/expired by a newer copy of the record
    '''
    def __init__(self, database, table_name, primary_id = None, primary_type = None, 
        primary_increment = None, auto_create=False, primary_length = PRIMARY_LENGTH, 
        default_length = DEFAULT_LENGTH, sort_by: dict[str, str]=None,
        deleted_ttl: int=DELETED_TTL,
    ):
        sort_by = sort_by or dict()
        if '_seq' not in sort_by: 
            sort_by['_seq'] = 'DESC'
        super().__init__(database, table_name, primary_id, primary_type, 
                         primary_increment, auto_create, primary_length, default_length,
                         sort_by)
        self._primary_increment = False
        if '_seq' not in self._primary_id: 
            self._primary_id = tuple([*self._primary_id, '_seq'])
            self._primary_type = tuple([*self._primary_type, 'int'])
        self.deleted_ttl = deleted_ttl

    def sync_columns(self, row, ensure=None, types=None):
        row['_del'] = 0
        return super().sync_columns(row, ensure, types)
    
    def cleanup(self, stream, columns):
        return super().cleanup(stream, columns)

    def insert_many(self, rows, chunk_size=CHUNKSIZE, sample_size=SAMPLE_SIZE, ensure=None, types=None, **kwds) -> int:
        self._sequence_no_local = seq = SequenceNumberMagic()
        # TODO: defend against overlap by checking max seq no in table
        rows = seq(rows)
        # TODO: add consideration for threading
        inserts = super().insert_many(rows, chunk_size=chunk_size, sample_size=sample_size, ensure=ensure, types=types)
        self.drop_duplicates(marker=seq.last)
        self.drop_expired()
        return inserts
    
    def drop_duplicates(self, marker: int=None): 
        '''Remove records that are active & duplicated based on the configured settings.
        
        Constructs a ROW_NUMBER over the primary_id columns sorting by `sort_by` properties, 
        always including _seq.
        For example: 
        ```
        UPDATE tbl SET _del=123
        WHERE _seq IN (
            SELECT _seq
            FROM tbl WHERE _del IS NULL 
            QUALIFY ROW_NUMBER() OVER(PART <primary id> ORD <sort by>) = 1
        )
        ```
        '''
        if not marker: 
            marker = SequenceNumberMagic().first
        tbl = self.table
        new_temp = self.db.create_temp_table
        part = (c for c in self._primary_id if c != '_seq')
        sort = (
            (tbl.c[col].asc() if sort_option.upper() == 'ASC' else tbl.c[col].desc())
            for col, sort_option in self._sort_by.items()
        )
        with new_temp('_seq', 'int') as tmp:
            row_num = func.row_number().over(partition_by=part, order_by=sort).label('_row_num')
            query = select(tbl.c._seq, row_num).filter(tbl.c._del == None).subquery()
            query = select(query.c._seq).filter(query.c._row_num > 1)
            stmt = tmp.table.insert().from_select(
                ('_seq',), 
                query
            )
            self.db.executable.execute(stmt)
            stmt = (
                tbl.update()
                    .where(tbl.c._seq.in_(
                            select(tmp._table.c._seq)
                                .subquery()
                        ))
                    .values(_del=marker)
            ) 
            self.db.executable.execute(stmt)

    def drop_expired(self):
        '''Remove records that are expired based on the `deleted_ttl` setting.'''
        secs = self.deleted_ttl
        if secs is None or secs < 0: 
            return
        marker = (datetime.now().timestamp() - secs) * 1_000_000
        self.delete(_del={'lt': marker})
    

class TypesPlus(ABC): 
    bigint = int = sqlalchemy.types.Numeric(38, 0)
    float = decimal = sqlalchemy.types.Numeric(20,10)
    str = String
    bytes = Binary
    datetime = sqlalchemy.types.DateTime
    date = sqlalchemy.types.Date
    bool = sqlalchemy.types.Boolean
    object = json = sqlalchemy.types.JSON

    def __init__(self, **kwds):
        super().__init__()
        self._cached = dict()
        for dtype_name, dtype in inspect.getmembers(self): 
            if is_instance_or_subclass(dtype, sqlalchemy.types.TypeEngine):
                self._cached[dtype_name] = dtype

    def uses_length(self, dtype: sqlalchemy.types.TypeEngine): 
        '''Determine if the passed type instance uses length-based types.'''
        return is_instance_or_subclass(dtype, (String, Binary))
    
    def guess(self, sample):
        """Given a single sample, guess the column type for the field.

        If the sample is an instance of an SQLAlchemy type, the type will be
        used instead.
        """
        if is_instance_or_subclass(sample, sqlalchemy.types.TypeEngine):
            return sample
        if isinstance(sample, str): 
            if dtype := self.check_type_string(sample):
                return dtype
            return self.str
        if isinstance(sample, bool):
            return self.bool
        elif isinstance(sample, int):
            return self.int
        elif isinstance(sample, (float, Decimal)):
            return self.float
        elif isinstance(sample, datetime):
            return self.datetime
        elif isinstance(sample, date):
            return self.date
        elif sample is None: 
            return None
        return self.json

    def check_type_string(self, dtype): 
        return self._cached.get(dtype)
TypesPlus.register(OriginalTypes)
    

def chunked_with_ceiling(stream, chunksize=-1): 
    '''Use `itertools.batched' but allow -1 to disable chunking.'''
    if chunksize == -1: 
        yield stream
    else: 
        yield from itertools.batched(stream, chunksize)


def is_instance_or_subclass(obj, class_or_tuple): 
    '''Check both `isinstance` and `issubclass` safely.'''
    try: 
        return issubclass(obj, class_or_tuple)
    except TypeError: 
        pass
    return isinstance(obj, class_or_tuple)


class enforce_columns: 
    '''Create a callable that will add columns to dictionaries input based on a sample.'''
    def __init__(self, columns=()): 
        self.columns = set(columns)

    def __call__(self, dict_like: dict): 
        actual = set(dict_like)
        missing = actual.difference(self.columns)
        for x in missing: 
            dict_like[x] = None
        return dict_like


class SequenceNumberMagic: 
    def __init__(self): 
        self.first = int(datetime.now().timestamp() * 1_000_000)
        self.last = self.first

    def __call__(self, stream: DataStream): 
        self.stream = stream
        return self

    def __iter__(self): 
        for row in self.stream: 
            row['_seq'] = self.next()
            yield row

    def next(self): 
        cur = self.last
        self.last += 1
        return cur
    