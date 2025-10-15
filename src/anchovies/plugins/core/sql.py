from gevent.lock import RLock as Lock
from datetime import datetime
from anchovies.sdk import (
    Uploader, Connection, ConnectionMemo, Tbl,
    source, sink, on_task, task, batch, session, info,
    AnchovySkip
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
    

class Uploader(Uploader): 
    '''A SQL uploader anchovy.
    
    This will continuously write data from the metastore to 
    a SQL Database configured using SQLAlchemy.
    Make sure to install the relevant SQLAlchemy driver for your
    database.
    '''
    db: Dataset = ConnectionMemo(Dataset, disconnected=True)
    # TODO: make sure Dataset() is not initialized by this
    schemamap = ConnectionMemo(SchemaMapper, env_id='schemas')

    def __init__(self):
        super().__init__()
        self.schemas = dict()

    @on_task('XOPEN') #TODO
    def check_for_work(self, **kwds): 
        '''Before opening a connection against'''
        self._wait_for_cleared = Lock()
        self.no_work = True
        self.ckpoint = session().metastore.checkpoints
        self.ckpoint.hint('*.datetime', des=datetime.fromisoformat, ser=datetime.isoformat)
        waiting = 0
        for tbl in batch().tbls or ():
            buf = IOBuff.open(anchovy_id=tbl.namespace, tbl=tbl.name, mode='rp')
            ckpoint = self.ckpoint[f'{tbl}.last.datetime']
            with buf: 
                buf.seek(ckpoint)
                waiting += len(tuple(buf))
            if waiting: 
                break
        if waiting: 
            info(f'At least {waiting:,} files are waiting for upload. Proceed!')
            self.no_work = False
        else: 
            for schema in self.schemas.copy():
                cnxn = self.schemas.pop(schema)
                cnxn.close()
            self.db = None
            
    @source()
    def default_source(self, tbl, **kwds): 
        ckpoint = self.ckpoint[f'{tbl}.last.timestamp']
        buf = self.data_buffer_cls.open(anchovy_id=tbl.namespace, tbl=tbl.name, mode='rl')
        buf.seek()
        for stream in buf: 
            yield from stream 
            with self._wait_for_cleared:
                ckpoint[f'{tbl}.last.timestamp'] = buf.tell()
                task().update(logical_timestamp=buf.tell()) #TODO

    @sink()
    def sql_sink(self, stream, tbl: Tbl, **kwds):
        if self.no_work: 
            raise AnchovySkip()
            # TODO: rework to use discover_tbls()
        has_data, stream = self.check_one(stream)
        if not has_data: 
            raise AnchovySkip() 
        schema = self.resolve_schema(tbl.namespace)
        dataset = self.get_or_connect_schema(schema)
        primary_keys = tuple(tbl.unique_by or tbl.set_by)
        # pk_setting, stream = dataset.guess_primary(primary_keys, stream)
        table = dataset.create_table(tbl.name, primary_id=primary_keys, primary_type='str')
        # TODO: should have a way to not default to type str???
        with self._wait_for_cleared, dataset:
            inserts = table.upsert_many(stream)
            task().update(inserts=inserts)
            # TODO: apply data retention at table level
            # TODO: add threading??? ++general performance testing

    def resolve_schema(self, name: str): 
        return self.schemamap.translate(name)
    
    def get_or_connect_schema(self, schema: str) -> Dataset: 
        if cnxn := self.schemas.get(schema): 
            return cnxn
        cnxn = self.connections.get_or_set('db', schema=schema) #TODO
        self.schemas[schema] = cnxn 
        return cnxn
