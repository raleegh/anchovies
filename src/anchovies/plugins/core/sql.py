import logging 
logger = logging.getLogger(__name__)
from gevent.lock import RLock as Lock
from datetime import datetime, UTC
from anchovies.sdk import Uploader as BaseUploader
from anchovies.sdk import (
    Connection, ConnectionMemo, Tbl, Task,
    source, sink, on_task, session, runtime, now, get_config,
)
from anchovies.plugins.core.dataset import Dataset
from anchovies.plugins.core.io import DefaultDataBuffer as IOBuff # TODO


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
    schemamap = ConnectionMemo(SchemaMapper, env_id='schemas')

    def __init__(self):
        super().__init__()
        self.schemas = dict()
        self.upload_batch_size = get_config('upload_batch_size', 1024 * 1024 * 8, astype=int)
        self._wait_for_cleared = Lock()

    def open(self, tbl): 
        '''Open the default data buffer against a Tbl.'''
        return runtime().DataBuffer(tbl , tbl.anchovy_id)

    def discover_tbls(self):
        waiting = 0
        self.ckpoint = session().datastore.checkpoints
        self.ckpoint.hint('*.timestamp', des=datetime.fromisoformat, ser=datetime.isoformat)
        for tbl in super().discover_tbls(): 
            ckpoint = self.ckpoint[tbl, 'last.timestamp']
            with self.open(tbl) as buf: 
                buf.seek(ckpoint)
                files = len(buf.readpaths(hint=self.upload_batch_size))
                logger.info(f'{files:,} waiting for tbl {repr(tbl)}')
                waiting += files
        self._waiting_file_count = waiting

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
        ckpoint = self.ckpoint[tbl, 'last.timestamp']
        with self.open(tbl) as buf: 
            buf.seek(ckpoint)
            stream = buf.read(self.upload_batch_size) 
            yield from stream 
            with self._wait_for_cleared:
                last_timestamp = buf.tell()
                if not last_timestamp.tzinfo: 
                    last_timestamp = last_timestamp.replace(tzinfo=UTC)
                lag = round((now() - last_timestamp.astimezone(UTC)).total_seconds() * 1000)
                ckpoint[tbl, 'last.timestamp'] = last_timestamp
                task.with_(last_timestamp=last_timestamp, lag=lag)

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
            ttl=tbl.deleted_ttl,
        )
        with self._wait_for_cleared, dataset:
            inserts = table.insert_many(stream)
            task.with_(inserts=inserts)
            # TODO: add threading??? ++general performance testing

    def resolve_schema(self, name: str): 
        return self.schemamap.translate(name)
    
    def get_or_connect_schema(self, schema: str) -> Dataset: 
        if cnxn := self.schemas.get(schema): 
            return cnxn
        cnxn = self.connections.get_or_set('db', schema=schema) #TODO
        self.schemas[schema] = cnxn 
        return cnxn
