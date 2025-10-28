import logging 
logger = logging.getLogger(__name__)
# from threading import RLock as Lock
from datetime import datetime, timedelta, UTC
from anchovies.sdk import Uploader as BaseUploader
from anchovies.sdk import (
    Connection, ConnectionMemo, Tbl, Task,
    source, sink, on_task, session, runtime, now, get_config, 
    getenv, bool_from_str, context,
)
from anchovies.plugins.core.dataset import Dataset


NO_SCHEMA_NAME = getenv('SQL_NO_SCHEMA_NAME', False, astype=bool_from_str)


class SchemaMapper(Connection): 
    '''
    Allow a user to "map" anchovy ids to schema names in the CNXN env var.

    Example: 
    ```
    ACHVY_CNXN__SCHEMAS__NEWNAME=anchovy1
    # this would translate data from `anchvoy1` -> `newname` schema
    ```
    '''
    def translate(self, anchovy_id: str): 
        return getattr(self, anchovy_id, anchovy_id)
    

class Uploader(BaseUploader): 
    '''A SQL uploader anchovy.
    
    This will continuously write data from the datastore to 
    a SQL Database configured using SQLAlchemy.
    Make sure to install the relevant SQLAlchemy driver for your
    database.

    This Uploader can be easily customized by overriding the db
    connection by subclassing a new type. Particularly, on Dataset, 
    the `insert_bulk()` method would be good to override.
    '''
    db: Dataset = ConnectionMemo(Dataset, disconnected=True)
    # TODO: make sure Dataset() is not initialized by this
    # to customize, all you would have to do is override this :)
    schemamap = ConnectionMemo(SchemaMapper, env_id='schemas', allow_missing=True)

    def __init__(self):
        super().__init__()
        self.schemas = dict()
        self.upload_batch_size = get_config('upload_batch_size', 1024 * 1024 * 8, astype=int)
        if self.upload_batch_size < 0: 
            self.upload_batch_size = None
        logger.info(
            'SQL upload limited by '
            + (f'{self.upload_batch_size:,}' if self.upload_batch_size else 'UNLIMITED')
            + ' bytes'
        )
        # self._wait_for_cleared = Lock()

    def open(self, tbl): 
        '''Open the default data buffer against a Tbl.'''
        return runtime().DataBuffer(tbl , tbl.anchovy_id)

    def discover_tbls(self):
        waiting = 0
        tbls = list()
        self.ckpoint = session().datastore.checkpoints
        self.ckpoint.hint('*.timestamp', des=datetime.fromisoformat, ser=datetime.isoformat)
        for tbl in super().discover_tbls(): 
            ckpoint = self.ckpoint[tbl.qualname, 'last.timestamp']
            with self.open(tbl) as buf: 
                buf.seek(ckpoint or 0)
                files = len(buf.readpaths(hint=self.upload_batch_size))
                logger.info(f'{files:,} waiting for tbl {repr(tbl)}')
                waiting += files
                if files: 
                    tbls.append(tbl)
        self._waiting_file_count = waiting
        return tbls

    @on_task('XOPEN')
    def check_for_work(self, **kwds): 
        '''Before opening a connection against'''
        # disconnect
        if not self._waiting_file_count:
            for schema in self.schemas.copy():
                cnxn = self.schemas.pop(schema)
                cnxn.close()
            self.db = None
            
    @source()
    def default_source(self, tbl: Tbl, task: Task, **kwds): 
        '''Customize the default source to stream from the default buffer
        with a data limit.
        '''
        ckpoint = self.ckpoint[tbl.qualname, 'last.timestamp']
        with self.open(tbl) as buf: 
            buf.seek(ckpoint or 0)
            stream = buf.read(self.upload_batch_size) 
            yield from stream 
        last_timestamp = buf.tell()
        task.on_success(
            self.ckpoint.serput, 
            (tbl.qualname, 'last.timestamp'), 
            last_timestamp + timedelta(milliseconds=1),
        )
        if not last_timestamp.tzinfo: 
            last_timestamp = last_timestamp.replace(tzinfo=UTC)
        lag = round((now() - last_timestamp.astimezone(UTC)).total_seconds())
        task.with_(last_stream_timestamp=last_timestamp, lag=lag)

    @sink()
    def sql_sink(self, stream, tbl: Tbl, task: Task, **kwds):
        '''Define a new SQL sink for all records.'''
        schema = self.resolve_schema(tbl.namespace)
        dataset = self.get_or_connect_schema(schema)
        if tbl.set_by: 
            raise NotImplementedError('The tbl.set_by option is not yet supported :(.')
        primary_keys = tuple(tbl.unique_by)
        primary_type, stream = dataset.watch_primary_type(primary_keys, stream)
        table = dataset.create_table(
            tbl.name, 
            primary_id=primary_keys, 
            primary_type=primary_type,
            sort_by=tbl.sort_by,
            deleted_ttl=tbl.deleted_ttl,
        )
        insert_kwds = {}
        if tbl.cols: 
            insert_kwds['types'] = dict(tbl.cols)
        with dataset:
            inserts = table.insert_many(stream, **insert_kwds)
        task.with_(inserts=inserts)
        # TODO: add threading??? ++general performance testing

    def resolve_schema(self, name: str): 
        '''Append anchovy context to schema name if not in prod.'''
        name = self.schemamap.translate(name)
        if context().anchovy_user != get_config('sql_prod_user', 'prod'): 
            name = name + '__' + context().anchovy_user
        return name
    # TODO: append env if not prod :)
    
    def get_or_connect_schema(self, schema: str) -> Dataset: 
        if cnxn := self.schemas.get(schema): 
            return cnxn
        kwds = dict(schema=schema)
        if NO_SCHEMA_NAME: 
            del kwds['schema']
        cnxn = self.connections.get_or_set('db', **kwds)
        self.schemas[schema] = cnxn 
        return cnxn
