from pytest import fixture
from anchovies.sdk import Operator, StreamingPlan, source, sink


MEM = 0
DISTINCT = set()
SAVESPACE = dict()
SEQUENCE = list()
WANT_ERROR = False


class TestException(Exception): 
    ...


class TestDownloader(Operator): 
    @source('sample')
    def generate_sample(self, **kwds): 
        yield {'id': 1}
        yield {'id': 2}
        yield {'id': 3}

    @source('other_sample')
    def generate_other_sample(self, **kwds): 
        yield {'id': 4}

    @source('sample_transf1')
    @sink('sample')
    def sink_sample_transf1(self, stream, **kwds): 
        for row in stream:
            row.update(new='new')
            yield row

    @source('sample_transf2')
    @sink('sample')
    @sink('other_sample')
    def sink_sample_transf2(self, stream, **kwds): 
        global MEM, DISTINCT
        for x in stream:
            MEM += 1
            DISTINCT.add(kwds['sink'])
            yield x

    @source('wildcard*')
    def get_wildcard(self, **kwds): 
        ...

    @source('nested')
    @sink('sample_transf1')
    def sink_nested(self, stream, **kwds): 
        global MEM 
        MEM += 1
        for row in stream:
            yield stream


class WildcardDownloader(Operator): 
    @source('src1')
    def gen_src1(self, **kwds): 
        yield {'id': 1}

    @source('src2')
    def gen_src2(self, **kwds): 
        yield {'id': 2}

    @sink()
    def sink_all(self, source, **kwds): 
        DISTINCT.add(str(source))


class OverrideDownloader(WildcardDownloader): 
    def sink_all(self, **kwds): 
        MEM += 1
    # this _should_ disable the sink


class SourceAllSinkAll(Operator): 
    @source()
    def generate(self, **kwds): 
        global MEM
        MEM += 1
        yield {'id': 1}

    @source('something')
    def gen_something(self, **kwds): 
        yield 1

    @sink()
    def default_sink(self, tbl, **kwds): 
        global DISTINCT
        DISTINCT.add(tbl)


class ReceiveTask(Operator): 
    @source()
    def get(self, task, **kwds): 
        yield None


class ComplexExecution(Operator): 
    @source('source1')
    def generate(self, **kwds):
        data = [
            dict(cursor=i)
            for i in range(10)
        ]
        for last in data: 
            SAVESPACE['last_from_source1'] = last
            SEQUENCE.append(last['cursor'])
            yield last
            SAVESPACE['next_from_source1'] = last

    @source('source2')
    @sink('source1')
    def sink_source2(self, stream, **kwds): 
        for last in stream: 
            SAVESPACE['last_from_source2'] = last
            SEQUENCE.append(last['cursor'])
            yield last
            SAVESPACE['next_from_source2'] = last

    @source('source3')
    @sink('source2')
    def sink_source3(self, stream, **kwds): 
        for i, last in enumerate(stream):
            if WANT_ERROR and i == 1: 
                raise TestException('Oh no!')
            SAVESPACE['last_from_source3'] = last
            SEQUENCE.append(last['cursor'])
            yield last
            SAVESPACE['next_from_source3'] = last


@fixture
def downloader(newmeta): 
    op = TestDownloader()
    yield op


@fixture
def wildcard_dwnldr(newmeta): 
    op = WildcardDownloader()
    yield op


@fixture
def override_dwnldr(newmeta): 
    op = OverrideDownloader()
    yield op


@fixture
def source_all_dwnldr(newmeta): 
    op = SourceAllSinkAll()
    yield op


@fixture
def receive_task_dwnldr(newmeta): 
    yield ReceiveTask()


@fixture
def complex_exec(newmeta): 
    yield ComplexExecution()


def test_still_callable(downloader): 
    assert list(downloader.generate_sample())
    assert list(downloader.sink_sample_transf1(downloader.generate_sample()))


def test_stream_guide_get(downloader): 
    guide = downloader.streams
    stream = guide.get('sample_transf2')
    assert stream.tbl_wildcard == 'sample_transf2'
    stream = guide.get('sample')
    assert stream.tbl_wildcard == 'sample'


def test_find(downloader): 
    guide = downloader.streams
    matches = list(guide.match('sample*'))
    assert len(matches) > 1


def test_multiple_wildcards(downloader): 
    guide = downloader.streams
    matches = list(guide.match('wildcard*'))
    assert len(matches) == 1


def test_run_simple(downloader):
    guide = downloader.streams
    guide['sample'].run()


def test_run_nested(downloader): 
    global MEM
    MEM = 0
    guide = downloader.streams
    guide['nested'].run()
    assert MEM
    

def test_multiple_sinks(downloader): 
    global MEM, DISTINCT
    MEM = 0
    DISTINCT = set()
    guide = downloader.streams
    guide['sample_transf2'].run()
    assert MEM >= 4
    assert len(DISTINCT) > 1


def test_wildcard_sink(wildcard_dwnldr): 
    global DISTINCT
    DISTINCT = set()
    guide = wildcard_dwnldr.streams
    StreamingPlan(guide.values()).run()
    assert len(DISTINCT) > 1


def test_override_wildcard_wink(override_dwnldr): 
    global MEM 
    MEM = 0
    guide = override_dwnldr.streams
    StreamingPlan(guide.values()).run()
    assert not MEM


def test_source_all_sink_all(source_all_dwnldr): 
    global MEM, DISTINCT
    MEM = 0
    DISTINCT = set()
    guide = source_all_dwnldr.streams
    StreamingPlan(guide.select(('table1', 'table2'))).run()
    assert MEM > 1, 'The @source() generator should be invoked... TWICE!'
    assert len(DISTINCT) > 1, 'The @sink() should be invoked... TWICE!'


def test_receive_task(receive_task_dwnldr): 
    guide = receive_task_dwnldr.streams
    StreamingPlan(guide.values()).run()


def test_linear_execution(complex_exec):
    '''This test ensures "linear execution".'''
    global WANT_ERROR
    WANT_ERROR = False
    global SEQUENCE
    SEQUENCE = list()
    guide = complex_exec.streams
    StreamingPlan(guide.values()).run()
    assert SEQUENCE[0:3] == [0, 0, 0]
    assert SEQUENCE[3:6] == [1, 1, 1]
    assert SEQUENCE[-3:] == [9, 9, 9]


def test_complex_exception_handling(complex_exec): 
    '''This test ensures that when an error occurs in a sink, the sources down from it
    are stopped, which ensures that you can use primitive logic when developing in
    anchovies. For example, i can track cursors in source1 and not worry about downstream
    failures because i won't be allowed to move ahead.
    '''
    global WANT_ERROR
    WANT_ERROR = True
    global SAVESPACE
    SAVESPACE = dict()
    guide = complex_exec.streams
    try:
        StreamingPlan(guide.values()).run()
    except TestException:
        pass
    # assert SAVESPACE['last_from_source1']['cursor'] == 0
    assert SAVESPACE['last_from_source2']['cursor'] == 1
    assert SAVESPACE['last_from_source3']['cursor'] == 0
    assert SAVESPACE['next_from_source1']['cursor'] == 0
    assert SAVESPACE['next_from_source2']['cursor'] == 1
    assert SAVESPACE['next_from_source3']['cursor'] == 0
    

def test_source_wildcard(source_all_dwnldr):
    guide = source_all_dwnldr.streams
    selection1 = guide.select(('something',))
    selection2 = guide.select(('something_else',))
    assert len(selection1) == 1
    assert len(selection2) == 1
    