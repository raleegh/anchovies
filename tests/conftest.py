from pytest import fixture
from anchovies.sdk import Datastore, Session, Downloader, Connection, ConnectionMemo


@fixture
def config_str():
    return '''
tbls: 
    sales: 
        cols: 
            order_num: int
            line_num: int
            upd_datetime: datetime
        set_by: order_num
        sort_by: line_num, -upd_datetime

    contacts: 
        cols: 
            contact_id: str
            first_name: str
            last_name: str
            email: str
            upd_datetime: datetime
        unique_by: contact_id
        sort_by: -upd_datetime
'''.strip()


class TestDownloader(Downloader): 
    ...


@fixture
def session(config_str): 
    with Session(
        TestDownloader, 
        anchovy_id='test', 
        anchovy_user='test', 
        config_str=config_str, 
        stream_queue_size=1,
        stream_chunk_size=1,
    ) as ses: 
        yield ses


@fixture
def newmeta(session): 
        with Datastore.new() as meta: 
            session.datastore = meta
            yield meta


@fixture
def downloader(session): 
    class TestConnection(Connection): 
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.is_connected = False
        def __enter__(self):
            self.is_connected = True
            return super().__enter__()
    class TestDownloader(Downloader): 
        test_cnxn: TestConnection
        test_no_connect=ConnectionMemo(TestConnection, disconnected=True)
        weird_cnxn=ConnectionMemo(TestConnection, env_id='diff_cnxn')
        json_cnxn: TestConnection
    yield TestDownloader()
