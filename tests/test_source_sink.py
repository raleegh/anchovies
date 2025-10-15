from pytest import fixture
from anchovies.sdk import Operator, StreamingPlan, source, sink


MEM = 0
DISTINCT = set()


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
        yield from stream

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
        yield from stream


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

    @sink()
    def default_sink(self, tbl, **kwds): 
        global DISTINCT
        DISTINCT.add(tbl)


@fixture
def downloader(): 
    op = TestDownloader()
    yield op


@fixture
def wildcard_dwnldr(): 
    op = WildcardDownloader()
    yield op


@fixture
def override_dwnldr(): 
    op = OverrideDownloader()
    yield op


@fixture
def source_all_dwnldr(): 
    op = SourceAllSinkAll()
    yield op


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
