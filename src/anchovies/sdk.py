import os
import sys
import socket
import contextvars as cvar
import json
import inspect
import io
import fnmatch
import threading as th
import logging
import logging.config
import importlib
import time
import atexit
from abc import ABC, abstractmethod
from collections import UserDict, UserList, namedtuple
from contextlib import ExitStack
from types import SimpleNamespace, ModuleType, MethodType
from typing import cast, Any, Literal, Iterator, Callable, Sequence
from functools import cache as memoize
from functools import cached_property as memoprop
from functools import partial
from datetime import datetime, UTC
from copy import copy, deepcopy

import gevent
import gevent.queue as gqueue
import yaml


__all__ = [
    # Exceptions
    'BaseAnchovyException', 
    'BadEnvironmentVariable', 
    'AnchovyExecutionTimeout',
    'AnchovyPluginNotFound',
    'AnchovyMissingConnection',
    'NoCheckpoint',
    'ExpectedJsonCompatibleType',
    'AnchovyStreamingError',
    'BrokenContext',
    'BrokenConfig',
    'BadMagicImport',
    # Environment
    'ID', 
    'USER',
    'HOME',
    'HOST',
    'PORT',
    'SESSION',
    'BATCH', 
    'CONTEXT',
    # 'TASK',
    'LOGGING_CONFIG_FILE',
    # Context magics
    'context',
    'session',
    'batch',
    # 'task',
    'Session',
    'Batch',
    # runtime magic
    'runtime',
    'get_config', 
    'anchovies_import',
    'import_runtime_from_config',
    # Downloaders & execution
    'Anchovy',
    'Downloader', 
    # Tbl info 
    'Cols', 
    'ColSelect', 
    'SortedColSelect',
    'Tbl',
    'TblSet',
    # Streaming API
    'Stream',
    'SourceStream',
    'SinkStream',
    'StreamGuide',
    'StreamingPlan',
    'source',
    'sink',
    # Task API
    'Task',
    'TaskGuide',
    'TaskCallback',
    'as_task',
    'on_task',
    # Connections
    'Connection',
    'ConnectionMemo',
    'ConnectionFairy',
    # Datastore
    'Datastore',
    'FileInfo',
    'Microservice',
    'BaseCheckpoint',
    'Checkpoint',
    'CachedCheckpoint',
    'TaskStore',
    'TblStore',
]


class BaseAnchovyException(Exception): ...
class BadEnvironmentVariable(BaseAnchovyException): ...
class AnchovyExecutionTimeout(BaseAnchovyException, TimeoutError): ...
class AnchovyPluginNotFound(BaseAnchovyException): ...
class AnchovyMissingConnection(BaseAnchovyException): ...
# class DatastoreKeyMissError(BaseAnchovyException): ...
class NoCheckpoint(BaseAnchovyException): ...
class ExpectedJsonCompatibleType(BaseAnchovyException): ...
class AnchovyStreamingError(BaseAnchovyException, RuntimeError): ...
class BrokenContext(BaseAnchovyException): ...
class BrokenConfig(BaseAnchovyException, RuntimeError): ...
class BadMagicImport(BaseAnchovyException, ImportError): ...
class TaskCallbackFailedWarning(Warning): ...


def getenv(var, default=None, *, astype: type=None): 
    val = os.getenv(f'ACHVY_{var}', default)
    if astype is None or val is None:
        return val
    try: 
        return astype(val)
    except Exception as e: 
        raise BadEnvironmentVariable(f'Environment var {var} was expected to parse to {astype}') from e
    

def findenv(prefix): 
    prefix = f'ACHVY_{prefix}'
    for var, val in os.environ.items(): 
        if var.startswith(prefix): 
            yield var.removeprefix(prefix), val


ID = getenv('ID')
USER = getenv('USER')
HOME = getenv('HOME', '~/.anchovies')
HOST = socket.gethostbyname(socket.gethostname())
PORT = getenv('PORT', '8080')
SESSION = cvar.ContextVar('SESSION', default=None)
CONTEXT = cvar.ContextVar('CONTEXT', default=None)
BATCH = cvar.ContextVar('BATCH', default=None)


logger = logging.getLogger('anchovies')
LOGGING_CONFIG_FILE = getenv('LOGGING_CONFIG_FILE')
info = logger.info
debug = logger.debug


def context() -> 'Session | Batch': 
    return CONTEXT.get()


def session() -> 'Session':
    return SESSION.get()


def batch() -> 'Batch': 
    return BATCH.get()


def now(): 
    return datetime.now(UTC)


def cached(key_prefix: str, *, ttl: float=-1, ttl_hook: Callable=None): 
    '''Cache an item in the internal cache.
    
    Parameters: 
    * ttl (float): set the time-to-live in seconds
    * ttl_hook (Callable): alternatively, provide a hook to retrieve TTL 
        at runtime
    '''
    def resolve_ttl(): 
        if ttl_hook is not None: 
            return ttl_hook() or ttl
        return ttl
    def decorator(func: Callable): 
        def cached_func(*args, **kwds): 
            cache = session().datastore.cache
            key = cache.make_key(*args, **kwds)
            if result := cache.get(key_prefix=key_prefix, key=key): 
                return result
            result = func(*args, **kwds)
            ttl = resolve_ttl()
            return cache.get_or_set(key_prefix=key_prefix, key=key, value=result, ttl=ttl)
        return cached_func
    return decorator


def get_config(setting_name: str, default=None, /, astype: type=None): 
    '''
    Scan both the Environment & Session for configs/settings. This
    will always return a str, UNLESS you use the `astype` arg, which 
    will safely attempt to convert the value.
    When the Environment & Session both have a value, the Session wins.
    '''
    from_env = getenv(setting_name.upper())
    from_ses = None
    if session():
        from_ses = session().config.get(setting_name)
    setting = from_ses if from_ses else from_env
    if setting and astype is not None: 
        try: 
            return astype(setting)
        except Exception as e: 
            raise BadEnvironmentVariable(f'Could not cast {setting} from {setting_name} to {astype}.') \
                from e
    return setting or default


def tuple_from_str(comma_sep_str: str) -> Sequence[str]: 
    if comma_sep_str and not isinstance(comma_sep_str, str):
        return tuple(comma_sep_str)
    if comma_sep_str: 
        return tuple(x.strip() for x in comma_sep_str.split(','))
    return ()


def bool_from_str(bool_str: str) -> bool: 
    if isinstance(bool_str, bool): 
        return bool_str
    if not bool_str.strip(): 
        return False
    return bool_str[0].upper() == 'T' or bool_str[0].upper() == 'Y'


def as_task(*task_args, capture=True, **task_kwds): 
    '''
    A helper function to make 
    decorating/accessing the task API inline easier.
    '''
    task_kwds.setdefault('task_type', 'TBL')
    maybe_callable = task_args[0]
    if callable(maybe_callable): 
        inline_decorator = True
        task_args = task_args[1:]
    def make_wrapper(callable):
        def wrapper(*args, **kwds): 
            res = None
            with Task.allowing_overflow(*task_args, **task_kwds) as task:
                kwds['task'] = task
                res = callable(*args, **kwds)
                if capture: 
                    task.with_(res)
            return res
        return wrapper
    if inline_decorator: 
        decorator = make_wrapper(maybe_callable)
    else: 
        def decorator(callable): 
            return make_wrapper(callable)
    return decorator


class Cols(UserDict):
    '''A column-data type definition list.'''
    def __init__(self, d: dict | str=None, /, **kwds): 
        if not d: 
            d = None
        if isinstance(d, str): 
            # a user can initialize a columns declaration
            #   with the form Cols('col1 int, col2 str')
            col_dtypes = [c.strip() for c in d.split(',')]
            d = {col: dtype for col, dtype in map(str.split, col_dtypes)}
        super().__init__(d, **kwds)

    def __str__(self): 
        return ', '.join(' '.join(map(str, col_dt)) for col_dt in self.items())

    def __repr__(self):
        return f'"{self}"'
    

class ColSelect(UserList):
    '''A selection of column names.'''
    def __init__(self, initlist=None):
        if not initlist: 
            initlist = None
        if isinstance(initlist, str): 
            # a user can initialize a columns declaration
            #   with the form ColSelect('col1, col2')
            initlist = [c.strip() for c in initlist.split(',')]
        super().__init__(initlist)

    def __str__(self):
        return ', '.join(self or ())
    
    def __repr__(self):
        return f'"{self}"'


class SortedColSelect(UserDict):
    '''A selection of column names, with sort order ("ASC"/"DESC").'''
    def __init__(self, d: dict | str=None, /, **kwds): 
        if not d: 
            d = None
        if isinstance(d, str): 
            # a user can initialize a columns declaration
            #   with the form SortedColSelect('-col1, col2')
            cols = [c.strip() for c in d.split(',')]
            d = {col.strip('-'): 'DESC' if col.startswith('-') else 'ASC' for col in cols}
        super().__init__(d, **kwds)

    def __str__(self): 
        return ', '.join(
            f'{"-" if col_dt[1] == "DESC" else ""}{col_dt[0]}' 
            for col_dt in self.items()
        )

    def __repr__(self):
        return f'"{self}"'


class Tbl: 
    '''A data-stream with tabular structure.
    
    Anchovies strictly uses MySQL/Oracle like naming on tables: meaning
    a table's namespace/schema IS the originating anchovy. So if a Tbl is 
    read and the namespace is "anchovy1", that Tbl is FROM anchovy1. This 
    doesn't seem like a major stress, but in theory multiple anchovies could
    touch the same table (by name), but in the namespace system, these would
    all be different tables because they are owned by different anchovies.

    For example:
    ```
    * anchovy1 - downloader
        * contacts
        * orders
        * order_line_items
    * anchovy2 - performing some kind of transform
        * order_line_items
    * anchovy3 - uploader
        * anchovy1.contact -> schema.contacts
        * anchovy1.orders -> schema.contact
        * anchovy2.order_line_items -> schema.order_line_items
        * ASSUMING, you have anchovy1/anchovy2 mapped to schema
    '''
    def __init__(
        self, 
        name: str,
        namespace: str | None=None,
        cols: Cols=None,
        unique_by: ColSelect=None,
        set_by: ColSelect=None,
        sort_by: SortedColSelect=None,
        deleted_ttl: int=None,
        # logical_timestamp
        **properties,
    ): 
        self.name = name
        self.namespace = namespace
        self.cols = Cols(cols) or Cols()
        self.unique_by = ColSelect(unique_by) or ColSelect()
        self.set_by = ColSelect(set_by) or ColSelect()
        self.sort_by = SortedColSelect(sort_by) or SortedColSelect()
        self.deleted_ttl = deleted_ttl
        self.properties = properties

    def __str__(self):
        return self.name

    def __repr__(self):
        return f'`{self.qualname}`'

    @property
    def qualname(self): 
        '''
        Returns a contextual "qualified name" for the table.

        In a downloader, this is just the table name.
        In an uploader, source tables will be qual'd with the 
        upstream anchovy id.
        Furthermore, tables will be "saved" ???
        '''
        return ((self.namespace or '') + '.' + self.name).strip('.')

    @property
    def anchovy_id(self) -> str | None: 
        return self.namespace or context().anchovy_id
    
    @anchovy_id.setter
    def anchovy_id(self, id): 
        self.namespace = id
    
    def info(self): 
        return {
            'name': self.name, 
            'namespace': self.namespace,
            'cols': str(self.cols),
            'unique_by': str(self.unique_by),
            'set_by': str(self.set_by),
            'sort_by': str(self.sort_by),
            **self.properties,
        }
    
    def dump(self): 
        return json.dumps(self.info(), default=AnchoviesEncoder().default)
    
    def merge(self, other): 
        '''Merge two Tbls, with the "other" arg winning.'''
        new = copy(self)
        for name, attr in inspect.getmembers(other): 
            if name.startswith('_'): 
                continue
            if attr:
                try:    
                    setattr(new, name, attr)
                except AttributeError: 
                    pass # is property
        # special combines
        new.properties.update(**other.properties)
        return new
    
    @classmethod
    def from_namespace(cls, namespace: str):
        '''Override the namespace argument permanently.'''
        def builder(**kwargs): 
            kwargs['namespace'] = namespace
            return cls(**kwargs)
        return builder


class TblSet(UserDict): 
    '''A set/list of tables.'''
    def __init__(self, d=None, /, *args):
        kwargs = {}
        if args: 
            kwargs = {t.name: t for t in args}
        super().__init__(**d or {}, **kwargs)

    def __repr__(self):
        return \
            '[' \
            +', '.join(repr(tbl) for tbl in self) \
            +']'

    def add(self, other: Tbl):
        if existing := self.data.get(other.qualname): 
            other = other.merge(existing)
        self.data[other.qualname] = other

    def extend(self, other: 'TblSet'): 
        for tbl in other.values(): 
            self.add(tbl)

    def merge(self, other: 'TblSet'): 
        '''Given another group of tables, combine them.
        
        Because of the behavior of `Tbl.merge`, the "other" argument
        will take precendence.
        '''
        new = type(self)(self)
        for tbl in new.values(): 
            tbl = deepcopy(tbl)
            if ltbl := other.get(str(tbl)): 
                tbl = tbl.with_(ltbl)
            self.add(tbl)
        for tbl in other.values(): 
            if tbl in new: 
                continue
            new.add(tbl)
        return new
    # TODO: extend behavior correctly


class Operator: 
    '''
    Basic execution class at the core of anchovies.

    Intended usage: 
    ```
    from sql.database.package import connect

    def NewDownloader(Downloader): 
        client = ConnectionMemo(connect)

        @source('some_table')
        def get_some_table(self, **kwds): 
            yield from self.client.run_query('SELECT * FROM some_table')
    ```
    This would automatically ingest returned dictionaries from the client method
    `some_table` into the default data buffer.

    You can also override the `discover_tbls()` method to further customize 
    table identification.
    '''
    def __init__(self): 
        self.streams = StreamGuide(self)
        self.task_callbacks = TaskGuide(self)
        self.connections = session().connections = ConnectionFairy(self)
        # connections acquired in startup

    def discover_tbls(self) -> Iterator[Tbl]:
        '''
        Discover `Tbl` instances associated to this Downloader.

        By default, this class checks config.yaml & the TblStore for 
        upstream & downstream tables. Override this method to customize
        what tables are processed, such as in cases of "dynamic discovery".
        '''
        yield from session().datastore.tbl_store

    def run_streams(self, selection: TblSet, **run_kwds):
        selected_streams = self.streams.select(selection)
        plan = StreamingPlan(selected_streams)
        plan.run(**run_kwds)


class Stream:
    '''
    A stream is the base unit of the source/sink apparatus 
    which allows data routing between methods at runtime.

    The Stream API should not be interacted with directly, rather
    use the `@source/@sink` decorators.
    '''
    def __init__(self, tbl_wildcard: str, func: Callable):
        super().__init__()
        self.tbl_wildcard = tbl_wildcard
        self.func = func
        self._outer = None
        self.maybe_mark_outer(func)
        self.operator: Downloader = None
        self.runtime_method: MethodType = None
        self.guide: StreamGuide = None
        self._stream: gqueue.SimpleQueue = None
        # TOOD: also add support for cache style queue
        self.included = set()

    def __str__(self): 
        return self.tbl_wildcard

    def __call__(self, *args, **kwds):
        func = self.func
        if self.runtime_method: 
            func = self.runtime_method
        return func(*args, **kwds)
    # TODO: i think actually, the __call__ method on this 
    #       should jump into execution rather than downgrading
    #       then, all streams receive a `stream` arg, either with 
    #       an it or None.
    #       
    # also, right now, this is designed to rely on itertools.tee, 
    #       but from reading the documentation i'm reconsidering
    # * should we batch thru iter1 -> for each child: child(batch)?
    # * should the ONLY execution method be "cached" (using pickle)?
    # * is there an option that can use gevent/async

    def run_as_stream(
        self, 
        *, 
        started_as: 'Stream'=None, 
        started_by: 'Stream'=None,
        wrapped_by: Callable=as_task, 
        **kwds,
    ): 
        # debug(f'node tree for {self} -->\n' + self.print_tree())
        if self.outer(): 
            raise AnchovyStreamingError(
                f'Stream {self} was scheduled even tho '
                'it is not the outermost stream.'
            )
            # stop execution when the wrong stream is called
        if started_as and isinstance(started_as, SinkStream): 
            kwds['sink'] = started_as
        if started_by and isinstance(started_by, SourceStream): 
            kwds['source'] = started_by
        tbl_name = None
        if started_as: 
            tbl_name = started_as.tbl_wildcard
        if started_by: 
            tbl_name = started_by.tbl_wildcard
        if not tbl_name:
            tbl_name = self.tbl_wildcard
        tbl = runtime().Tbl(tbl_name)
        if str(tbl) in session().datastore.tbl_store: 
            tbl = session().datastore.tbl_store[str(tbl)]
        kwds = dict(stream=self._stream, tbl=tbl, **kwds)
        # TODO: consider passing self as stream...
        should_wrap = False
        if isinstance(self, SourceStream): 
            should_wrap = True
        if not should_wrap or not wrapped_by: 
            return self.actually_run_as_stream(**kwds)
        kwds['wrapped_by'] = wrapped_by
        new_callable = wrapped_by(self.actually_run_as_stream, **kwds)
        return new_callable(**kwds)

    def actually_run_as_stream(self, **kwds): 
        possible_iterator = self(**kwds)
        self.maybe_iter(possible_iterator)

    def maybe_iter(self, it): 
        if isinstance(it, Iterator): 
            for i in it: 
                gevent.sleep()

    def maybe_mark_outer(self, func: Callable): 
        if not isinstance(func, Stream): 
            return
        func._outer = self

    def outer(self): 
        cur = self
        ret = None
        while outer := cur._outer: 
            ret = outer
            cur = outer
        return ret
    
    def maybe_outer(self) -> 'Stream': 
        return self.outer() or self
    
    def outermost_run_as_stream(self, *, include=(), **kwds): 
        exe = self.maybe_outer()
        exe.include(include)
        exe.run_as_stream(started_as=self, **kwds)
    
    def make_ready(self): 
        self._stream = gqueue.SimpleQueue()
    
    def make_method(self, operator: Operator): 
        method = MethodType(self.func, operator)
        self.save_runtime_method(operator, method)
        return method

    def save_runtime_method(self, op, meth): 
        self.operator = op
        self.runtime_method = meth

    def get_substreams(self): 
        maybe_substream = self.func 
        while True: 
            if not isinstance(maybe_substream, Stream): 
                return
            yield maybe_substream
            maybe_substream = maybe_substream.func

    def put(self, obj): 
        if self._stream: 
            self._stream.put(obj)

    def include(self, seq=()):
        for maybe_include in seq: 
            if maybe_include is self: 
                continue
            path = set(maybe_include.path())
            if self not in path: 
                continue
            self.included.add(maybe_include)

    def clone(self, new_name: str):
        new = copy(self)
        new.tbl_wildcard = new_name
        return new


class SourceStream(Stream): 
    '''A streaming component that RECEIVES data.'''
    def __repr__(self): 
        return f'@source(`{self.tbl_wildcard}`)'
    
    def actually_run_as_stream(self, **kwds):
        if 'source' not in kwds:
            kwds['source'] = self
        sinks = self._sinks = tuple(self.guide.sinks.match(
            str(self),
            include=self.included,
        )) or ()
        for sink in sinks: 
            sink.maybe_outer().make_ready()
        self.futures = cast(list[gevent.Greenlet], list())
        for sink in sinks: 
            ctx = cvar.copy_context()
            fut = gevent.Greenlet(
                # sink.outermost_run_as_stream, 
                ctx.run,
                sink.outermost_run_as_stream,
                started_by=self, 
                include=self.included,
            )
            fut.start()
            self.futures.append(fut)
        super().actually_run_as_stream(**kwds)
        for fut in self.futures: 
            fut.get() # raise exception/wait for finish :)

    def maybe_iter(self, it):
        for i in it:
            for sink in self._sinks: 
                sink.maybe_outer().put(i)
                gevent.sleep()
        for sink in self._sinks:
            sink.maybe_outer().put(StopIteration)
    
    def is_root(self): 
        return len(list(self.path())) == 1

    def run(self): 
        '''Utility method opening a new `StreamingPlan` on one stream.'''
        return StreamingPlan((self,)).run()
    
    def ipath(self): 
        seen = set()
        def maybe_yield(candidate): 
            if not isinstance(candidate, SourceStream): 
                return
            if candidate not in seen: 
                seen.add(candidate)
                yield candidate
        yield from maybe_yield(self)
        for stream in self.get_substreams(): 
            if isinstance(stream, SinkStream):
                if maybe_upstream := tuple(self.guide.match(str(stream))): 
                    for x in maybe_upstream: 
                        yield from maybe_yield(x)
                        for y in x.ipath(): 
                            yield from maybe_yield(y)

    @memoize
    def path(self): 
        return tuple(self.ipath())
    
    def iroots(self) -> Iterator[Stream]: 
        for x in self.path():
            if x.is_root(): 
                yield x

    def roots(self): 
        '''Pull the "root" of this stream. Could return `Self`.'''
        return tuple(self.iroots())
  
    # def downstream(self): 
    #     for d in self.guide.sinks.get(str(self)): 
    #         yield d


class SinkStream(Stream): 
    '''A streaming component that RECEIVES data.'''
    def __init__(self, tbl_wildcard, func):
        if isinstance(func, SourceStream): 
            raise SyntaxError(f'A @sink cant decorate a @source {func}!')
        super().__init__(tbl_wildcard, func)

    def __repr__(self): 
        return f'@sink(`{self.tbl_wildcard}`)'
    
    def __call__(self, *args, **kwds):
        if 'sink' not in kwds:
            kwds['sink'] = self
        return super().__call__(*args, **kwds)
    
    def prev(self): 
        yield from self.guide.match(str(self))


def source(tbl_wildcard: str=None): 
    '''
    Decorate a method to schedule execution of that 
    method as a "@source".

    Sources should be UNIQUE in the scope of your downloader
    based on wildcard name. Omitting the name argument will 
    setup a global source via "*".
    Sources should always handle all key-word arguments, such as by
    using `**kwds`.
    '''
    tbl_wildcard = tbl_wildcard or '*'
    def decorator(func): 
        return SourceStream(tbl_wildcard, func)
    return decorator


def sink(tbl_wildcard: str=None): 
    '''
    Decorate a method to schedule execution of that 
    method as a "@sink".

    Sinks are outputs from a named source. Sources need 
    not be unique. By default, a default sink (`Downloader.default_sink()`)
    is configured with `@sink('*')`. This can be disabled by 
    overriding such as: 
    ```
    # @sink() ## REMOVE
    def default_sink(self, **kwds):
        pass
    ```
    Sinks should always handle all key-word arguments, such as by
    using `**kwds`.
    '''
    tbl_wildcard = tbl_wildcard or '*'
    def decorator(func): 
        return SinkStream(tbl_wildcard, func)
    return decorator


class StreamGuide(UserDict): 
    '''
    A coordinator instance to help manage the execution of streams.
    
    All "@source" instances get key'd in via tbl/wildcard name, forcing uniqueness.
    Sinks do not need to be unique. Retrieve a @source in order to run it & it's ancestors.
    If you select all the @sources you want to run, and pass to `StreamingPlan`, the planner 
    will only run each node in the graph once.
    '''
    def __init__(self, operator: 'Downloader'): 
        super().__init__()
        self.operator = self.op = operator
        self.streams = set()
        self.sources = self.data
        for attr, stream in inspect.getmembers(self.op): 
            if isinstance(stream, Stream): 
                self.save_stream(stream)
                for sink in stream.get_substreams(): 
                    self.save_stream(sink)
                setattr(self.op, attr, stream.make_method(self.op))
        self.sinks = SinkGuide(self)

    def save_stream(self, stream: Stream): 
        if stream in self.streams: 
            return
        stream.guide = self
        self.streams.add(stream)
        if isinstance(stream, SourceStream): 
            self.sources[str(stream)] = stream

    def get(self, key) -> 'SourceStream': 
        return super().get(key)
    
    def match(self, pat) -> Iterator[SourceStream]: 
        if exact := self.get(pat):
            yield exact
        for name, stream in self.copy().items(): 
            if (
                fnmatch.fnmatch(name, pat) 
                or fnmatch.fnmatch(pat, name)
            ) and stream is not exact:
                translation_tbl = str.maketrans('', '', '*?[]')
                if name != name.translate(translation_tbl):
                    # this means we matched a wildcard on the selection, so need to yield the name back out
                    new_stream = stream.clone(pat)
                    self.save_stream(new_stream)
                    yield new_stream
                    continue
                yield stream

    def select(self, selection) -> Sequence[Stream]: 
        res = set()
        for possible_match in selection: 
            for match in self.match(str(possible_match)): 
                res.add(match)
        return tuple(res)
    

class SinkGuide(UserDict): 
    '''A helper for the `StreamGuide` to collect `SinkStream`s.'''
    def __init__(self, guide: StreamGuide): 
        super().__init__()
        self.guide = guide
        sinks = list()
        for maybe_sink in self.guide.streams: 
            if not isinstance(maybe_sink, SinkStream): 
                continue
            sinks.append(maybe_sink)
        for sink in sinks: 
            for source in sink.prev(): 
                self.map(source, sink)
    
    def map(self, source: SourceStream, sink: SinkStream): 
        if str(source) not in self: 
            self[str(source)] = list()
        self[str(source)].append(sink)

    def match(self, pat, *, include=None) -> Iterator[SinkStream]: 
        def yield_included(sinks): 
            for sink in sinks: 
                outer = sink.maybe_outer()
                if isinstance(outer, SinkStream): 
                    yield sink 
                    continue
                    # always run sinks
                if include is not None and outer not in include: 
                    continue
                yield sink
        if exact := self.get(pat):
            yield from yield_included(exact)
        it = tuple(self.items())
        it = tuple(filter(
            lambda n: \
                (fnmatch.fnmatch(n[0], pat)
                    or fnmatch.fnmatch(pat, n[0])
                ) \
                and n[1] is not exact, 
            it
        ))
        for name, stream in it: 
            yield from yield_included(stream)


class StreamingPlan: 
    '''
    Initialize like a list, and then hit `run()` in order to execute 
    each exactly once.
    '''
    def __init__(self, streams: Sequence=()):
        self.streams = tuple(streams)
        self.seen = ()
        self.please_include = ()

    def iter_streams(self): 
        seen = self.seen = set()
        please_include = self.please_include = set()
        for stream in self.streams:
            please_include.update(set(stream.path()))
        for stream in sorted(self.streams, key=lambda s: str(s)): 
            if stream in seen: 
                continue
            yield stream

    def schedule_and_complete(self, stream: SourceStream, **stream_kwds):
        for root in stream.roots():
            root.include(self.please_include)
            root.run_as_stream(**stream_kwds)
            self.seen.add(root)
            self.seen.update(root.included)

    def run(self, **stream_kwds): 
        for stream in self.iter_streams(): 
            self.schedule_and_complete(stream, **stream_kwds)



class Downloader(Operator): 
    '''
    Basic execution class at the core of anchovies.

    Intended usage: 
    ```
    from sql.database.package import connect

    def NewDownloader(Downloader): 
        client = ConnectionMemo(connect)

        @source('some_table')
        def get_some_table(self, **kwds): 
            yield from self.client.run_query('SELECT * FROM some_table')
    ```
    This would automatically ingest returned dictionaries from the client method
    `some_table` into the default data buffer.

    You can also override the `discover_tbls()` method to further customize 
    table identification.
    '''    
    def __init__(self):
        super().__init__()
        self.data_buffer_cls = runtime().DataBuffer

    @sink()
    def default_sink(self, stream, tbl, **kwds): 
        buf = self.data_buffer_cls(tbl)
        with buf: 
            buf.writelines(stream)


class Uploader(Operator): 
    '''
    Basic execution class at the core of anchovies.

    Intended usage: 
    ```
    from sql.database.package import connect

    def NewDownloader(Downloader): 
        client = ConnectionMemo(connect)

        @source('some_table')
        def get_some_table(self, **kwds): 
            yield from self.client.run_query('SELECT * FROM some_table')
    ```
    This would automatically ingest returned dictionaries from the client method
    `some_table` into the default data buffer.

    You can also override the `discover_tbls()` method to further customize 
    table identification.
    '''
    def __init__(self):
        super().__init__()
        self.data_buffer_cls = runtime().DataBuffer

    @source()
    def default_source(self, tbl: Tbl, **kwds): 
        db = context().datastore.with_(anchovy_id=tbl.anchovy_id)
        buf = self.data_buffer_cls(tbl.name, datastore=db)
        with buf: 
            yield from buf


class Connection(SimpleNamespace):
    '''
    An empty/shell connection class.
    
    Usage: 
    ```
    class MyDownloader(Connection): 
        conn: Connection
    ```
    Believe it or not, but `conn` will be acquired at 
    runtime based on matching environment variables.
    For example, `ACHVY_CNXN__CONN__ARG1` would go to 
    `Connection(arg1=?)`.

    You could also overload an ABC registration on a class
    to force a connection: 
    ```
    class CustomConnection(SomeConnectionClass, ABC): 
        pass
    CustomConnection.register(anchovies.sdk.Connection)
    ```
    '''
    def __enter__(self): 
        return self
    def __exit__(self, *args): ...


class ConnectionMemo:
    '''
    An annotation helper for guiding the `ConnectionFairy` 
    to make a connection, even if it can't be annotated like
    a vanilla `Connection`.

    Usage: 
    ```
    from other.package import connect 

    class MyDownloader(Downloader): 
        conn = ConnectionMemo(connect)
    ```
    '''
    def __init__(self, cls: type, *, disconnected=False, env_id: str=None): 
        self.cls = cls
        self.disconnected = disconnected
        self.env_id = env_id

    def __call__(self, *args, **kwds):
        return self.cls(*args, **kwds)


class ConnectionFairy:
    '''
    This class attaches and manages Connection instances in the Session and 
    Downloader.
    '''
    def __init__(self, operator: Downloader): 
        self.operator = self.op = operator
        self.ids = dict(self.find_cnxn_annotations())
        self.data = dict()

    def __enter__(self): 
        return self.connect()

    def __exit__(self, *args): 
        self.close()

    def connect(self): 
        self._cm_stack = ExitStack()
        connect = self._cm_stack.enter_context
        for id, memo in self.ids.items(): 
            if memo.disconnected: 
                continue
            if cnxn := self.get_or_set(id):
                self.attach(id, connect(cnxn))
        return self
    
    def close(self): 
        self._cm_stack.close()

    def find_cnxn_annotations(self) -> tuple[str, type[Connection]]: 
        suspected_cnxns = list()
        for attr, annotation in inspect.get_annotations(self.op.__class__).items(): 
            if issubclass(annotation, Connection):
                annotation = ConnectionMemo(annotation)
                suspected_cnxns.append((attr, annotation))
        for attr, val in inspect.getmembers(self.op): 
            if isinstance(val, ConnectionMemo): 
                suspected_cnxns.append((attr, val))
        return tuple(suspected_cnxns)
    
    def find(self): 
        for id in self.ids:
            self.get_or_set(id)

    @cached('cnxn.open', ttl_hook=lambda: get_config('cnxn_ttl', -1, astype=int))
    def get_or_set(self, id, **cnxn_settings) -> Connection | None: 
        '''Retrieve an open connection or create it new!'''
        attrs = dict(self.find_args_for_cnxn(id))
        if not attrs: 
            raise AnchovyMissingConnection(
                f'{self.op.__class__} was expecting '
                f'connection with id {id}!'
            )
        cnxn = self.ids[id](**attrs, **cnxn_settings)
        self.attach(id, cnxn)

    def attach(self, id: str, cnxn: Connection): 
        self.data[id] = cnxn
        setattr(self.op, id, cnxn)

    def find_args_for_cnxn(self, id): 
        id = self.ids[id].env_id or id
        for k, v in findenv(f'CNXN__{id}__'.upper()): 
            if k.lower() == 'jsondata': 
                data = json.loads(v)
                for k, v in data.items(): 
                    yield k.lower(), v
                continue
            yield k.lower(), v


class Anchovy:
    '''A single executor.'''
    def __init__(self, id: str=None, user: str=None): 
        self.id = id or ID or f'{HOST}:{PORT}'
        self.user = user or USER or self.default_username()
        self._session = None

    def __str__(self): 
        return f'Anchovy({self.user}/{self.id})'

    @staticmethod
    def default_username(): 
        '''Derive a username for the process based on login'''
        return os.getenv('USER') \
            or os.getenv('USERNAME') \
            or socket.gethostname().removesuffix('.local')

    def run(self, operator_cls: type[Downloader], /, **session_kwds): 
        ses = runtime().Session(operator_cls, self, **session_kwds)
        return self.run_with_session(ses, operator_cls, **session_kwds)
    
    def run_with_session(self, session, operator_cls: type[Downloader], /, **session_kwds): 
        with session:
            for batch in session.iter_batches(): 
                batch()
        return session.results()
    
    def run_with_exception_handling(self, operator_cls: type[Downloader], /, **session_kwds) \
            -> 'tuple[SessionResult, Exception | None]': 
        exception = None
        results = None
        ses = runtime().Session(operator_cls, self, **session_kwds)
        try: 
            results = self.run_with_session(ses, operator_cls, **session_kwds)
        except Exception as e: 
            exception = e
        if not results:
            results = ses.results()
        return results, exception

    def list_checkpoints(self, path_or_glob, operator_cls=Downloader, **session_kwds): 
        '''Find associated checkpoints & list them.
        
        Assuming invoked by application, so therefore: starting
        an InteractiveSession and moving right along.
        '''
        ses = self.make_interactive_session(operator_cls, **session_kwds) # TODO: would like this cleaner
        yield from ses.datastore.checkpoints.find(path_or_glob)

    def delete_checkpoints(self, path_or_glob, operator_cls=Downloader, **session_kwds): 
        '''Remove a single checkpoint for an anchovy.'''
        ses = self.make_interactive_session(operator_cls, **session_kwds)
        keys = tuple(dict(ses.datastore.checkpoints.find(path_or_glob)))
        for key in keys:
            ses.datastore.checkpoints.remove(key)

    def make_interactive_session(self, operator_cls, **session_kwds) -> 'InteractiveSession': 
        if self._session: 
            return session()
        ses = self._session = InteractiveSession(operator_cls, self, **session_kwds)
        ses.startup()
        return ses

class Plugin: 
    '''A plugin module from `anchovies.plugins`.
    
    This class doesn't get used so much as for ensuring all 
    installed datastores get recognized.
    '''
    def __init__(self, name, module): 
        self.name = name 
        self.module = module

    @memoize
    @staticmethod
    def discover(plugin: str=None): 
        '''Walk `anchovies.plugins` and import all.'''
        plugin_list = anchovies_import(plugin)
        plugins = dict()
        for name, possible_plugin in inspect.getmembers(plugin_list):
            full_path = '.'.join((plugin or '', name)).strip('.')
            if name.startswith('__'): 
                continue 
            if isinstance(possible_plugin, ModuleType):
                if not possible_plugin.__name__.startswith('anchovies'): 
                    continue
                plugins[full_path] = Plugin(full_path, possible_plugin)
                found = Plugin.discover(full_path)
                plugins.update(**found)
        return plugins
            

@memoize
def anchovies_import(path: str=None): 
    '''
    Import an object in the `anchovies` plugin-space.

    ```
    >>> anchovies_import('TaskStore') 
    ... <anchovies.sdk.TaskStore>
    >>> anchovies_import('core.s3.S3Datastore')
    ... <anchovies.plugins.core.s3.S3Datastore>
    ```
    '''
    default_exception = BadMagicImport(f'The following magic import could not be found: "{path}".')
    dynamic_import_exception = None
    global_import_exception = None
    try: 
        if inspect.isclass(path): 
            return path
        full_path = f'anchovies.plugins.{path}' if path else 'anchovies.plugins'
        try: 
            full_path, cls = '.'.join(full_path.split('.')[:-1]), full_path.split('.')[-1]
            if cls:
                module = importlib.import_module(full_path)
                return getattr(module, cls)
        except Exception as e: 
            dynamic_import_exception = e
        # attempt importing from global
        try:
            return globals()[path]
        except Exception as e: 
            global_import_exception = e
        if dynamic_import_exception: 
            raise dynamic_import_exception
        if global_import_exception: 
            raise global_import_exception
    except Exception as e: 
        raise default_exception from e
    raise default_exception


REGISTERED_DATASTORES = list()
FileInfo = namedtuple('FileInfo', ('path', 'modified_at', 'size'))
# TODO: enhance the FileInfo class


class Metastore(ABC): 
    '''A store that provides some contextual help to Datastore.
    
    This micro-manages things such as anchovy id & user. Additionally, 
    sets up microservices from config.
    '''
    def __init__(self, *, upstream=(), namespace: str=None, **kwds):
        super().__init__()
        self._applied_anchovy_user = None
        self._applied_anchovy_id = None
        # want upstream to be easy to control at this level, 
        #   ergo, orphan from session
        self.upstream = upstream or session().upstream or ()
        self.namespace = namespace
        # setup checkpoints
        db = self.make_datastore_for_microservice('checkpoints')
        self.checkpoints = runtime().Checkpoint(db)
        # setup task store
        db = self.make_datastore_for_microservice('task_store')
        self.task_store = runtime().TaskStore(db)
        # setup tbl store
        db = self.make_datastore_for_microservice('tbl_store')
        self.tbl_store = runtime().TblStore(db)
        # setup results
        db = self.make_datastore_for_microservice('result_store')
        self.result_store = runtime().ResultStore(db)
        # setup cache
        db = self.make_datastore_for_microservice('cache')
        self.cache = runtime().Cache(db)

    @property
    def anchovy_user(self): 
        '''Current anchovy user, within the scope of the store.
        
        The store's user and the context user _may_ be different.
        '''
        if self._applied_anchovy_user: 
            return self._applied_anchovy_user
        return context().anchovy_user
    
    @anchovy_user.setter
    def anchovy_user(self, user): 
        self._applied_anchovy_user = user

    @property
    def anchovy_id(self): 
        '''Current anchovy id, within the scope of the store.
        
        The store's id and the context id _may_ be different.
        '''
        if self._applied_anchovy_id: 
            return self._applied_anchovy_id
        return context().anchovy_id

    @anchovy_id.setter
    def anchovy_id(self, user): 
        self._applied_anchovy_id = user

    def context_home(self, *paths): 
        '''Return the relative path for the active user.'''
        path = ''
        if self.anchovy_user: 
            path += '/' + self.anchovy_user
        if not path: 
            raise BrokenConfig('Unplanned scenario')
            #TODO: what should be done when no path?
        return (path.strip('/') + '/' + '/'.join(paths)).strip('/')
    
    def anchovy_home(self, *paths): 
        '''Return the relative path for the active anchovy.'''
        return self.context_home(self.anchovy_id, *paths)
    
    def connect(self):
        '''Open the store & sub services.'''
        self._exit_stack = ExitStack().__enter__()
        enter = self._exit_stack.enter_context
        self.checkpoints = enter(self.checkpoints)
        self.task_store = enter(self.task_store)
        self.tbl_store = enter(self.tbl_store)
        return self
    
    def close(self):
        '''Disconnect the store & sub services.
        
        Subservices frequently use `flush()` methods, 
        so the main goal is to give them a chance to do so.
        '''
        self._exit_stack.__exit__(*sys.exc_info())

    def __enter__(self): 
        return self.connect()
    
    def __exit__(self, *args): 
        self.close()

    def with_(self, **kwds): 
        '''Clone the metastore, and assign new values.
        
        For example: 
        ```
        new = metastore.with_(anchovy_id='other_anchovy')
        ```
        This opens a metastore viewing a DIFFERENT anchovy.
        '''
        new = copy(self)
        for k,v in kwds.items(): 
            setattr(new, k, v)
        return new
    
    def make_datastore_for_microservice(self, id: str): 
        '''Provided a microservice ID, make or return self based on config.'''
        config = get_config(f'{id}_datastore')
        if config is None: 
            return self
        return Datastore.new(config)
    # TODO: cache so connect once
    #  not doing right now because have to make sure each only 
    #  gets closed _once_ as well... easier said than done


class Datastore(Metastore): 
    '''A store that manages persistent storage for anchovies.'''
    def __init__(self, path, **kwds):
        super().__init__(**kwds)
        path = path or get_config('datastore', HOME)
        self.original_path = self.origpath = path

    @staticmethod
    def register(cls): 
        '''Add a datastore to the data store registration.'''
        global REGISTERED_DATASTORES
        if cls not in REGISTERED_DATASTORES: 
            REGISTERED_DATASTORES.append(cls)
        return cls

    @staticmethod
    def meta_list_datastores(): 
        '''List registered datastores.'''
        Plugin.discover()  # force walk of entire directory
        yield from REGISTERED_DATASTORES

    @classmethod
    def new(self, path=None, **kwds) -> 'Datastore': 
        '''Assemble a new Datastore, checking against all plugins
        based on the Datastore uri.
        '''
        # TODO: cache?
        from anchovies.plugins.core.filesystem import FilesystemDatastore
        if path is None:
            return FilesystemDatastore()
        for metastore_cls in self.meta_list_datastores(): 
            if metastore_cls.is_compatible(path): 
                return metastore_cls(path, **kwds)
        raise AnchovyPluginNotFound(
            f'There was no metastore registration found matching {path}.\n'
            'Here are the runtime registrations:\n' + '\n'.join(repr(m) for m in self.meta_list_datastores())
        )

    @staticmethod
    @abstractmethod
    def is_compatible(path): 
        '''Check a path for compatibility.'''
        ...

    def open(self, path, mode: Literal['rb', 'wb', 'r', 'w']=None, **kwds) -> io.BufferedRandom:
        '''Open a read/write stream on a file, potentially in text mode.'''
        mode = mode or 'r'
        assert mode in {'rb', 'wb', 'r', 'w'}
        if mode in {'r', 'w'}: 
            return io.TextIOWrapper(self.openb(path, mode), **kwds)
        return self.openb(path, mode.replace('b', ''), **kwds)
    
    @abstractmethod
    def openb(self, path, mode: Literal['r', 'w']=None, **kwds) -> io.BytesIO:
        '''Open a read or write stream on a file, strictly in binary mode.'''
        ...

    def read(self, path) -> str:
        '''Call `read()` on an underlying file.'''
        with self.open(path, 'r') as buf: 
            return buf.read()
        
    def write(self, path, buf: str): 
        '''Call `write()` on an underlying file.'''
        with self.open(path, 'w') as f: 
            return f.write(buf)

    @abstractmethod
    def list_files(self, relpathglob=None, *, after=None, before=None):
        '''Iterate through matching file names, optionally filtering 
        by modification time.
        '''
        ...

    def list_objs(self, relpathglob=None, *, cls=None, **kwds): 
        '''Wrapper on `list_files()` allowing consistent serialization of objects.'''
        stream = self.list_files(relpathglob, **kwds)
        stream = map(self.read, stream)
        stream = map(json.loads, stream)
        for data in stream:
            yield cls(**data)

    @abstractmethod
    def describe(self, relpath) -> FileInfo:
        '''Describe the matching file.'''
        ...

    @abstractmethod
    def delete(self, path) -> None:
        '''Delete the file (if it exists).'''
        ...


class Microservice(ABC):
    '''An anchovies construct that helps with persistent storage
    on a particular facet of anchovies data.
    ''' 
    def __init__(self, db: Datastore):
        super().__init__()
        self.db = db


class BaseCheckpoint(Microservice): 
    '''Keep & maintain checkpoints on your anchovies.
    
    Usage: 
    ```
    ts = context().checkpoint[tbl, 'last.timestamp']
    client.query(after=ts)
    ...
    context().checkpoint[tbl, 'last.timestamp'] = new_ts
    ```
    '''
    def __init__(self, db):
        super().__init__(db)
        self.ser_hints = dict()
        self.des_hints = dict()
    
    def open(self): 
        '''Allow context-handling.'''
        pass

    def close(self): 
        pass

    def __enter__(self): 
        self.open()
        return self

    def __exit__(self, *args): 
        self.close()

    def hint(self, key, *, ser: Callable=None, des: Callable=None): 
        '''Provide a type hint (and ser/des) for a string pattern of key.'''
        if ser: # SERIALIZE TO TEXT
            self.ser_hints[key] = ser
        if des: # DESERIALIZE FROM TEXT
            self.des_hints[key] = des

    @staticmethod
    def get_serdes(serdes: dict, key: str) -> Callable: 
        '''Find matching serdes.'''
        if ser := serdes.get(key): 
            return ser
        for pat, ser in serdes.items(): 
            if fnmatch.fnmatch(key, pat): 
                return ser
        return lambda x: x # do nothing
        
    def get_ser(self, key):
        '''Get a serializer for a given key.''' 
        return self.get_serdes(self.ser_hints, key)
    
    def get_des(self, key): 
        '''Get a deserializer for a given key.''' 
        return self.get_serdes(self.des_hints, key)

    @abstractmethod
    def items(self) -> Iterator[tuple[str, str]]: 
        '''Iterate through all items in the checkpoint store.'''
        ...

    def find(self, path_or_glob: str) -> Iterator[tuple[str, str]]: 
        '''Call against `.items()` and filter by pattern matching on key.'''
        for key, value in self.items(): 
            if fnmatch.fnmatch(key, path_or_glob): 
                yield key, value

    @abstractmethod
    def get(self, key: str | Sequence, *, strict=False): 
        '''Get a specific checkpoint by key.'''
        ...

    @abstractmethod
    def put(self, key: str, data: str):
        '''Update a specific checkpoint by key.'''
        ...

    @abstractmethod
    def remove(self, key: str): 
        '''Remove a specific key'''
        ...

    def desget(self, key, **kwds):
        '''Get a specific checkpoint & deserialize.'''
        des = self.get_des(key)
        return des(self.get(key, **kwds))
    
    def serput(self, key, data, **kwds): 
        '''Update a specific checkpoint & serialize.'''
        ser = self.get_ser(key)
        return self.put(key, ser(data), **kwds)

    def __getitem__(self, key):
        '''Get a specific checkpoint & deserialize.'''
        key = self.make_key(key)
        return self.desget(key)
    
    def __setitem__(self, key, data): 
        '''Update a specific checkpoint & serialize.'''
        key = self.make_key(key)
        return self.serput(key, data)
    
    def __delitem__(self, key): 
        '''Delete an item.'''
        key = self.make_key(key)
        return self.remove(key)

    def as_path(self, key: str): 
        '''Return the path for a particular checkpoint.'''
        return self.db.anchovy_home('$checkpoints', key)
    
    def from_path(self, path: str): 
        '''Parse the path for a particular checkpoint.'''
        return path.removeprefix(self.db.anchovy_home('$checkpoints') + '/')
    
    def make_key(self, key: Sequence[str]): 
        '''Because some methods support tuple keys, make a safe to use key.'''
        if isinstance(key, str): 
            return key
        key = '.'.join(map(str, key))
        return key


class Checkpoint(BaseCheckpoint):     
    def items(self):
        debug(f'read checkpoints at {self.as_path("*")}')
        for path in self.db.list_files(self.as_path('*')): 
            yield self.from_path(path), self.db.read(path)

    def get(self, key, *, strict=False):
        try: 
            return self.db.read(self.as_path(key))
        except Exception as e: 
            if strict: 
                raise NoCheckpoint(e) from e
            return None
        
    def put(self, key, data): 
        return self.db.write(self.as_path(key), data)
    
    def remove(self, key): 
        self.db.delete(self.as_path(key))
    

class CachedCheckpoint(Checkpoint): 
    def __init__(self, db):
        super().__init__(db)
        self.cached = False
        self.data = dict()
        self._changed = set()

    def maybe_cache(self): 
        if self.cached: 
            return
        self.data = dict(super().items())
        self.cached = True

    def flush(self):
        for key in self._changed: 
            self.db.write(self.as_path(key), self.data[key]) 
        self._changed = set()

    def get(self, key, *, strict=False):
        val = self.data.get(key)
        if val is None and strict: 
            raise NoCheckpoint(key)
        return val
    
    def put(self, key, data): 
        self.data[key] = data
        self._changed.add(key)

    def remove(self, key):
        if key in self.data:
            del self.data[key]
        return super().remove(key)

    def items(self): 
        return self.data.items()

    def open(self):
        self.maybe_cache()

    def close(self): 
        self.flush()
                
    def __repr__(self):
        return (
            'Checkpoint('
            +', '.join(
                f'{key}={repr(val)}'
                for key, val in self.items()
            )+
            ')'
        )


class TaskStore(Microservice):
    """
    Help store & manage task log instances.

    Example: 
    ```
    tasks = TaskStore(meta).open()
    with Task() as tsk: 
        ... do work ...
        tsk.with_(results)
        # where results is some kind of "results tuple" or set of information
    ```
    """
    def __init__(self, db):
        super().__init__(db)
        self._cvtoken = None
        self._log_num = 0
        self._lock = th.Lock() # TODO: does this need to be gevent?

    def open(self): 
        '''Connect to the task store.'''
        self._cvtoken = OPEN_TASK_STORE.set(self)

    def close(self): 
        '''Disconnect from the task store.'''
        if self._cvtoken:
            OPEN_TASK_STORE.reset(self._cvtoken)

    def __enter__(self): 
        self.open()
        return self

    def __exit__(self, *args): 
        self.close()

    def send(self, task: 'Task'): 
        '''Send a task to the storage location.'''
        path = self.new_path()
        self.db.write(path, task.dump())

    def new_path(self, key=None):
        '''Generate a new path like `$task_logs/YYYmmdd<...>.json`.'''
        with self._lock:
            key = key or context().batch_guid + '.' + str(self._log_num).zfill(4)
            self._log_num += 1
            return self.db.anchovy_home('$task_logs', f'{key}.json')


OPEN_TASK_STORE = cvar.ContextVar('OPEN_TASK_STORE')
# no default instance :(


TaskTypeT = Literal['STARTUP', 'XOPEN', 'TABLE', 'XCLOSE', 'ERROR', 'SHUTDOWN']
ExecStatusT = Literal['OK', 'ERR']
class Task: 
    '''
    Execution of an arbitrary datum of work within anchovies.

    See the [README](/README.md) for more info about Task behaviors.
    '''
    def __init__(
        self,
        tbl: str=None,
        anchovy_id: str=None, 
        anchovy_user: str=None,
        session_id: str=None,
        batch_id: str=None, 
        thread_id: str=None,
        operator: str=None, 
        task_type: TaskTypeT=None, 
        duration: int=0,
        timestamp: datetime=None,
        data: dict | list | None=None, 
        status: ExecStatusT=None,
    ): 
        self.anchovy_id = anchovy_id
        self.anchovy_user = anchovy_user
        self.session_id = session_id
        self.batch_id = batch_id
        self.thread_id = thread_id
        self.operator = operator
        self.task_type = task_type
        self.duration = duration
        self.timestamp = timestamp
        self.tbl = tbl
        self.data = data
        self.status = status
        self.apply_defaults()
        self._ctoken = None
        self._lock = th.Lock() # TODO: does this need to be gevent lock???

    def __str__(self): 
        label = str(self.task_type)
        if self.tbl: 
            label = label + '-->' + str(self.tbl)
        return f'@task({label})'

    def __enter__(self): 
        return self.start()
    
    def __exit__(self, exctype, *args): 
        self.send()
        if exctype and issubclass(exctype, Exception): 
            if session().batch_policy == 'CONTINUE':
                # this surpresses the error if it's not a shutdown
                return True

    @staticmethod
    def get_thread_id(): 
        if g := gevent.getcurrent(): 
            return f'Greenlet-{id(g)}'
        if t := th.get_ident(): 
            return f'Thread-{t}'
        
    @classmethod
    def allowing_overflow(cls, *args, **kwds): 
        # TODO: handle args overflow???
        signature = inspect.signature(cls.__init__)
        new_kwds = {k: v for k,v in kwds.items() if k in signature.parameters}
        return cls(*args, **new_kwds)

    def apply_defaults(self): 
        ctx = context()
        nvl = lambda x: getattr(self, x) or getattr(ctx, x)
        self.anchovy_id = nvl('anchovy_id')
        self.anchovy_user = nvl('anchovy_user')
        self.session_id = nvl('session_id')
        self.batch_id = nvl('batch_id')
        self.thread_id = self.thread_id or self.get_thread_id()
        # self.operator = nvl('operator')
        self.operator = self.operator or ctx.operator_cls
        if self.operator and not isinstance(self.operator, str): 
            self.operator = self.operator.__name__
        self.task_type = cast(TaskTypeT, self.task_type or 'NA')
        self.duration = self.duration or 0
        self.timestamp = self.timestamp or now()
        self.status = cast(ExecStatusT, self.status or 'OK')
        if sys.exc_info()[0]: 
            self.status = 'ERR'
        if self.tbl: 
            self.task_type = 'TABLE'

    def start(self):
        '''Start a task for monitoring.'''
        self.started_at = now()
        debug(f'start task {self} @ {self.started_at.isoformat()}')
        return self

    def send(self):
        '''Record the task to the task store.
        
        This also "finalizes" the Task record by compiling it 
        correctly for serialization, such as adding timestamps
        & checking for errors.
        '''
        op = session().maybe_make_operator()
        for callback in op.task_callbacks.for_task(self.task_type):
            try:
                callback(task=self)
            except Exception as e: 
                raise TaskCallbackFailedWarning(e) from e
                # TODO: will this suffice
        try:
            self.completed_at = now()
            if context().batch_id and not self.batch_id: 
                self.batch_id = context().batch_id
            self.maybe_convert_to_exception()
            if not self.duration: 
                self.duration = round((self.completed_at - self.timestamp).total_seconds() * 1000)
            msg = 'completed'
            if self.status == 'ERR': 
                msg = 'failed'
            info(f'{self} {msg} in {self.duration/1000:,}s @ {self.completed_at}')
            self.timestamp = self.completed_at
            context().task_store().send(self)
            self.mark_done()
        except BrokenConfig: ...
        except BrokenContext: 
            info(
                f'Your anchovy could not be run due to misconfiguration.'
            )
            raise
        except BaseException: 
            self.convert_to_exception()
            self.mark_done()
            raise

    def convert_to_exception(self): 
        self.with_(sys.exc_info()[1])
        self.status = 'ERR'

    def maybe_convert_to_exception(self): 
        if sys.exc_info()[0]:
            self.convert_to_exception()

    def mark_done(self): 
        if self.task_type == 'STARTUP': 
            session().startup_task = self 
            return
        if self.task_type == 'SHUTDOWN': 
            session().shutdown_task = self
        batch().add_done_task(self)

    def with_(self, data: object | dict | list | str = None, **kwds): 
        '''Add data to the dynamic `data` attribute of the task log.'''
        if isinstance(data, Exception): 
            data = {
                'err_type': type(data).__name__,
                'err_msg': str(data),
            }
        if not isinstance(data, (dict, list, str)): 
            new = {
                k: v for k,v in inspect.getmembers(data)
                if not callable(v)
                    and not k.startswith('_')
            }
            data = new
        if kwds: 
            if not data:  
                data = dict()
            data.update(**kwds)
        with self._lock: 
            if not self.data: 
                self.data = {}
            self.data.update(data)
        return data

    def info(self): 
        info = {
            k: v 
            for k,v in inspect.getmembers(self)
            if not callable(v)
                and not k.startswith('_')
                and k != 'anchovy'
        }
        d = info['data']
        if d and not isinstance(d, (dict, list, str)): 
            raise ExpectedJsonCompatibleType(d)
        if d: 
            try: 
                d = json.dumps(d, default=AnchoviesEncoder().default)
            except Exception as e: 
                raise ExpectedJsonCompatibleType(d) from e
        return info
    
    def slim_info(self): 
        return {
            'thread_id': self.thread_id, 
            'operator': self.operator, 
            'status': self.status,
            'duration': self.duration,
            'timestamp': self.timestamp,
            'task_type': self.task_type,
            'tbl': self.tbl,
            'data': self.data, 
        }
    
    def dump(self): 
        info = self.info()
        return json.dumps(info, default=AnchoviesEncoder().default)


def on_task(task_name: TaskTypeT): 
    '''Schedule a callback to be executed AFTER a task completes.
    
    Usage: 
    ```
    class CustomDownloader(Downloader): 
        @on_task('XOPEN')
        def do_at_start(self): 
            ...
    ```
    '''
    def decorator(func): 
        return TaskCallback(task_name, func)
    return decorator


class TaskCallback: 
    '''When a task completes, callback to the function mentioned here.'''
    def __init__(self, task_name: TaskTypeT, func: Callable):
        self.task_name = task_name
        self.func = func
        self.guide: TaskGuide = None

    def make_method(self, operator: Operator): 
        method = MethodType(self.func, operator)
        self.save_runtime_method(operator, method)
        return method

    def save_runtime_method(self, op, meth): 
        self.operator = op
        self.runtime_method = meth

    def __call__(self, *args, **kwds):
        func = self.func
        if self.runtime_method: 
            func = self.runtime_method
        return func(*args, **kwds)


class TaskGuide:
    '''Create a manager to correctly setup task callbacks for an
    Operator.
    ''' 
    def __init__(self, op: Operator):
        self.op = Operator
        for attr, task in inspect.getmembers(self.op): 
            if isinstance(task, TaskCallback): 
                self.save_task(task)
                setattr(self.op, attr, task.make_method(self.op))
        self.tasks = set()
        self.task_map = dict()

    def save_task(self, task: TaskCallback): 
        '''Record a decorated task to the bank.'''
        if task in self.tasks: 
            return
        task.guide = self
        self.tasks.add(task)
        if task.task_name not in self.task_map: 
            self.task_map[task.task_name] = list()
        self.task_map[task.task_name].append(task)

    def for_task(self, task_type: TaskTypeT): 
        '''Return matching callbacks for a specific task type.'''
        return self.task_map.get(task_type) or ()
    

class TblStore(Microservice):
    '''Persistent storage of `Tbl` constructs.'''
    def __iter__(self): 
        yield from self.tbls.values()

    def __enter__(self): 
        return self.open()
    
    def __exit__(self, *args): 
        return self.flush()
    
    def __contains__(self, tbl): 
        return self.tbls.__contains__(tbl)
    
    def __getitem__(self, tbl): 
        return self.tbls.__getitem__(tbl)

    def open(self):
        '''Connect to the TblStore.'''
        tbls = self.read_upstream()
        # tbls = tbls.merge(self.read_db())
        tbls = tbls.merge(self.read_config())
        self.tbls = tbls
        return self
    
    def flush(self):
        '''Save all pending tbls.'''
        for tbl in self: 
            self.save(tbl)

    def save(self, tbl: Tbl): 
        '''Save a specific Tbl to Datastore.'''
        path = self.new_path(tbl)
        self.db.write(path, tbl.dump())

    def new_path(self, tbl: Tbl): 
        '''`$HOME/user/id/$tables/<< qualified table name >>.json`'''
        return self.db.anchovy_home('$tables', f'{tbl}.json')

    # @memoize
    def read_upstream(self): 
        '''Check "upstream" anchovies for configured tables.'''
        tbls = TblSet()
        upstream = self.db.upstream
        metas = map(
            lambda achvy: self.db.with_(
                anchovy_id=achvy,
                namespace=achvy,
                upstream=(),
            ), 
            upstream,
        )
        dbs = map(TblStore, metas)
        for db in dbs: 
            new = db.read_db()
            tbls.extend(new)
        return tbls
    
    def read_db(self): 
        '''
        For TblStores opened as an "upstream" store, 
        read the filesystem/metastore.
        '''
        tbls = TblSet()
        cls = runtime().Tbl.from_namespace(self.db.anchovy_id)
        for tbl in self.db.list_objs(self.new_path('*'), cls=cls): 
            # ??? need this to somehow know which anchovy it came from
            if not tbl.namespace:
                tbl.namespace = self.db.namespace
            tbls.add(tbl)
        return tbls

    # @memoize
    def read_config(self): 
        '''
        Read the `config.yaml` for table definitions.
        
        Any data in `config.yaml` OVERWRITES upstream properties.
        '''
        tbls = TblSet()
        for name, data in (context().config_yaml.get('tbls') or {}).items():
            tbl = runtime().Tbl(name=name, **data)
            tbls.add(tbl)
        return tbls


class Cache(Microservice): 
    '''Provide an internal methodology for doing TTL caching.
    
    Because the ultimate goal of anchovies is building long-running
    services, caching could be helpful/allow more intuitive programming.
    For example, consider the base SQL Uploader: it checks for modified 
    file paths in the BATCH OPEN task **and** once for each table. 
    If this is naturally cached, then we've used one less round trip against
    object storage.
    '''
    def __init__(self, db):
        super().__init__(db)
        self._prefixes = dict()

    def get(self, key: str, *, key_prefix: str=None) -> Any:
        return self.check_ttl(self.prefix(key_prefix).get(key))

    def get_or_set(self, key: str, value: Any=None, *, key_prefix: str=None, ttl: int=-1) -> Any:
        '''Set a value in the cache and retrieve.'''
        self.prefix(key_prefix)[key] = self.add_ttl(value, ttl)
        return value
    
    def prefix(self, key_prefix: str): 
        '''Get a key prefix db.'''
        if key_prefix not in self._prefixes:
            self._prefixes[key_prefix] = dict()
        return self._prefixes.get(key_prefix)
    
    def check_ttl(self, value: tuple[Any, int]): 
        '''Check the expires at property.'''
        if value is None: 
            return None
        if value[1] and value[1] > time.time(): 
            return None
        debug(f'cache hit! {value} expiring in {time.time() - value[1]}')
        return value[0]
    
    def add_ttl(self, value, ttl):
        '''Add an expires at property.''' 
        expires_at = None
        if ttl > -1: 
            expires_at = time.time() + ttl
        return (value, expires_at)
    
    def make_key(self, *args, **kwds): 
        named_args = tuple((k, v) for k,v in kwds.items())
        return hash((*args, *named_args))


class ResultStore(Microservice): 
    '''Dump session results & provide access for anchovies to the same
    with potentially source specific results files. 
    '''
    def dump(self, path, data: dict | list | str): 
        '''Commit a results file.'''
        if isinstance(data, (dict, list, tuple)): 
            data = json.dumps(data)
        self.db.write(self.qualified(path), data)

    def qualified(self, path: str): 
        '''`$HOME/$user/$id/$results/<path>`'''
        return self.db.anchovy_home('$results', path)
    
    def __setitem__(self, path: str, data): 
        return self.dump(path, data)


class BaseContext: 
    '''Common ancestor context class for Session & Batch.'''
    def __init__(
        self, 
        operator_cls: type[Downloader], 
        anchovy: Anchovy=None,
        *,
        anchovy_id: str | None=None, 
        anchovy_user: str | None=None,
        **config,
    ): 
        self.config = config
        self.operator_cls = Downloader
        if operator_cls: 
            self.operator_cls = operator_cls
            # TODO: should this be moved to "config"?
        self.anchovy = anchovy or \
            Anchovy(anchovy_id, anchovy_user)
        self._session_id = None
        self._batch_id = None
        self._last_batch_id = None
        # self._worker_id = '0'
    
    @property
    def anchovy_id(self): 
        return self.anchovy.id
    
    @anchovy_id.setter
    def anchovy_id(self, id): 
        self.anchovy.id = id
    
    @property
    def anchovy_user(self): 
        return self.anchovy.user
    
    @anchovy_user.setter
    def anchovy_user(self, user): 
        self.anchovy.user = user
    
    @property
    def session_id(self) -> str: 
        return self._session_id
    
    @property
    def batch_id(self) -> str: 
        return self._batch_id
    
    @property
    def batch_guid(self): 
        return session().session_id + '-' + (self.batch_id or self.next_batch_id())
     
    @staticmethod
    def new_batch_id(): 
        ses = session()
        batch_id = ses.next_batch_id()
        ses._last_batch_id = int(batch_id)
        return batch_id
    
    @staticmethod
    def next_batch_id(): 
        ses = session()
        batch_id = 0
        if ses._last_batch_id is not None: 
            batch_id = ses._last_batch_id + 1
        return str(batch_id).zfill(3)
    
    def task_store(self): 
        try: 
            return OPEN_TASK_STORE.get()
        except Exception as e: 
            raise BrokenContext(
                'A connection to the Task Store (and likely the Metastore) ' \
                'did not exist at the time of invocation.'
            )
    

class InteractiveSession(BaseContext): 
    '''A Session instance used by the CLI.'''
    def __init__(
        self, 
        operator_cls, 
        anchovy=None,
        *,
        config_str: str=None,
        config_file_loc: str=None,
        **config, 
    ):
        super().__init__(operator_cls, anchovy, **config)
        self.start_context() # do early to allow reading from session in get_config()
        self._config_str = config_str or config.get('config')
        self._config_file_loc = config_file_loc or config.get('config_file')
        self.upstream: Sequence[str] = get_config('upstream', astype=tuple_from_str)
        self.enabled: Sequence[str] = get_config('enabled', astype=tuple_from_str) or ()
        self.enabled_set = set(self.enabled)
        self.disabled: Sequence[str] = get_config('disabled', astype=tuple_from_str) or ()
        self.disabled_set = set(self.disabled)
        self.is_task_executor = get_config('is_task_executor', True, astype=bool_from_str)
        # helpers
        self.connections: ConnectionFairy | None = None
        self.datastore: Datastore | None = None
        self._session_id = self.new_session_id()
        self._operator = None
        self.batches: list[Batch] = list()
        self.has_started = False
        self.has_shutdown = False
        self.startup_at: datetime | None = None
        self.startup_task: Task | None = None
        self.shutdown_at: datetime | None = None
        self.shutdown_task: Task | None=None
        self.execution_timeout: float = get_config('execution_timeout', astype=float)
        self.batch_policy: Literal['CONTINUE', 'RAISE'] = get_config('batch_policy', 'CONTINUE')
        self.log_level = log_level = get_config('log_level', 'INFO')
        log_level_num = getattr(logging, log_level)
        assert log_level_num, f'Could not find "{log_level}" in logging module.'
        logging.basicConfig(
            level=logging.CRITICAL,
            format='%(asctime)s %(levelname)s %(message)s -- (%(name)s)',
        )
        if LOGGING_CONFIG_FILE: 
            logging.config.fileConfig(LOGGING_CONFIG_FILE)
        logger.setLevel(log_level_num)
        self._lock = th.Lock()

    def __repr__(self):
        return f'{type(self).__name__}-{self.session_id}'

    @memoprop
    def config_yaml(self) -> dict: 
        try:
            data = {}
            if self._config_file_loc: 
                assert self._config_file_loc
                with open(self._config_file_loc) as f:
                    data = yaml.safe_load(f) 
            if self._config_str and not self._config_file_loc: 
                data = yaml.safe_load(self._config_str)
            assert isinstance(data, dict)
            return data
        except Exception as e: 
            raise BrokenConfig(e) from e
        
    @staticmethod
    def new_session_id(): 
        '''String id of format like "20250203044523"'''
        return datetime.now(UTC).strftime('%Y%m%d%H%M%S')

    def startup(self): 
        if self.has_started: 
            return self
        op = self.maybe_make_operator() # should this receive args?
        if not self.datastore:
            self.datastore = Datastore.new(**self.dump())
            self.datastore.connect()
        atexit.register(self.garbage_collection_hook)
        self.has_started = True
   
    def garbage_collection_hook(self, *args, **kwds): 
        self.shutdown()

    def start_context(self): 
        self._session_token = SESSION.set(self)
        self._context_token = CONTEXT.set(self)

    def shutdown(self):
        if self.has_shutdown: 
            return
        debug(f'Shutdown session {self}')
        SESSION.reset(self._session_token)
        CONTEXT.reset(self._context_token)
        self._session_token, self._context_token = None, None
        self.datastore.close() # should this be closed?
        self.has_shutdown = True

    def __enter__(self) -> 'Session': 
        return self.startup()

    def __exit__(self, *args): 
        self.shutdown()

    def iter_batches(self):
        '''
        Generate new batches to execute.
        
        Runs one iteration in task-based execution.
        '''
        while True: 
            bt = runtime().Batch(self)
            self.batches.append(bt)
            yield bt
            if self.is_task_executor: 
                break

    def dump(self): 
        return {
            k: v 
            for k,v in inspect.getmembers(self)
            if not k.startswith('_') and not callable(v)
        }
    
    def maybe_make_operator(self) -> Downloader:
        if self._operator: 
            return self._operator
        cls = anchovies_import(self.operator_cls)
        self._operator = cls()
        return self._operator

    def select_tables(self, tbls=()) -> TblSet: 
        final_tbls = TblSet()
        for tbl in tbls: 
            assert isinstance(tbl, Tbl)
            if self.is_disabled_tbl(tbl) or not self.is_enabled_tbl(tbl): 
                continue
            final_tbls.add(tbl)
        debug(f'enabled tables for session --> {final_tbls}')
        return final_tbls
    
    def is_disabled_tbl(self, tbl): 
        if not self.disabled: 
            return False
        return str(tbl) in self.disabled_set

    def is_enabled_tbl(self, tbl): 
        if not self.enabled: 
            return True
        return str(tbl) in self.enabled_set
    
    def results(self): 
        return SessionResult(self)
    

class RuntimeSession(InteractiveSession): 
    '''A session instance used by the `anchovy run` command.'''
    def startup(self):
        return as_task(self.actually_startup, task_type='STARTUP', capture=False)() 
    
    def actually_startup(self, **task_kwds): 
        if self.has_started: 
            return self
        info('')
        info('𓆝 𓆟 𓆞 𓆟 𓆝 '*8)
        info('𓆟 𓆝 𓆝 𓆟 𓆞 '*8)
        info('')
        info(f'starting up {self} (STARTUP)...')
        self.startup_at = now()
        if not self.datastore:
            self.datastore = Datastore.new(**self.dump())
            self.datastore.connect()
        op = self.maybe_make_operator() # should this receive args?
        self.connections = ConnectionFairy(op)
        self.connections.connect()
        self.has_started = True
        return self
      
    def shutdown(self): 
        if self.has_shutdown: 
            return
        as_task(self.actually_shutdown, task_type='SHUTDOWN', capture=False)()
        self.datastore.result_store['anchovy_run_results.json'] = self.results().dump()
        self.datastore.close()
        super().shutdown()

    def actually_shutdown(self, **task_kwds):
        info(f'shutting down {self} (SHUTDOWN)...')
        self.shutdown_at = now()
        self.connections.close()
        self.has_shutdown = True
Session = RuntimeSession


class SessionResult: 
    def __init__(self, sesobj: Session):
        self.anchovy_id = sesobj.anchovy_id
        self.anchovy_user = sesobj.anchovy_user
        self.session_id = sesobj.session_id
        self.startup_at = sesobj.startup_at
        self.startup_task = sesobj.startup_task.slim_info() if sesobj.startup_task else None
        self.shutdown_at = sesobj.shutdown_at
        self.shutdown_task = sesobj.shutdown_task.slim_info() if sesobj.shutdown_task else None
        self.batches = list(bt.slim_info() for bt in sesobj.batches)
        self.status = self.infer_status()
        
    def infer_status(self): 
        for bt in self.batches: 
            if bt['status'] != 'OK': 
                return 'ERR'
        if self.startup_task and self.startup_task['status'] != 'OK': 
            return 'ERR'
        if self.shutdown_task and self.shutdown_task['status'] != 'OK': 
            return 'ERR'
        return 'OK'
    
    def info(self): 
        return {
            'anchovy_id': self.anchovy_id,
            'anchovy_user': self.anchovy_user, 
            'startup_at': self.startup_at, 
            'startup_task': self.startup_task,
            'shutdown_at': self.shutdown_at, 
            'shutdown_task': self.shutdown_task, 
            'batches': self.batches, 
            'status': self.status,
        }

    def dump(self): 
        return pretty_json_dump(self.info())
    

def pretty_json_dump(data): 
    return json.dumps(
        data,
        indent=2,
        separators=(', ', ': '),
        default=AnchoviesEncoder().default,
    )
    

class DefaultSession(Session): ...
DEFAULT_SESSION = DefaultSession(None, anchovy_id='default')
SESSION.set(DEFAULT_SESSION)
CONTEXT.set(DEFAULT_SESSION)


class Batch(BaseContext): 
    def __init__(self, session: Session): 
        self.session = session
        self.tbls: TblSet | None = None
        self._batch_id = self.new_batch_id()
        self._context_token = None
        self._batch_token = None
        self.done_tasks: list[Task] = list()
        self._lock = th.Lock()
    
    @property
    def operator_cls(self): 
        return self.session.operator_cls
    
    @property
    def anchovy(self): 
        return self.session.anchovy

    @property
    def session_id(self): 
        return self.session.session_id
    
    @property
    def datastore(self): 
        return self.session.datastore
    
    def __repr__(self):
        return f'Batch-{self.batch_id}'
    
    def __enter__(self): 
        return self.open()
    
    def __exit__(self, *args): 
        self.close()

    def __call__(self): 
        self.run()

    def run(self): 
        timeout = self.session.execution_timeout
        timeout_lbl = f'{timeout:,}s' if timeout else 'UNLIMITED'
        with self:
            try:
                fut = gevent.Greenlet(self.actually_run)
                debug(f'start batch future with timeout --> {timeout_lbl}')
                fut.run()
                fut.get(timeout=timeout)
            except gevent.Timeout as e: 
                fut.kill()
                raise AnchovyExecutionTimeout(
                    f'The batch {self} did not complete in the expected timeout'
                    f' of {timeout} seconds.'
                ) from e

    def actually_run(self): 
        op = session().maybe_make_operator()
        tbls = op.discover_tbls()  # TODO: cache discover tables?
        tbls = self.tbls = session().select_tables(tbls)
        op.run_streams(tbls)

    def open(self):
        return as_task(self.actually_open, task_type='XOPEN', capture=False)()
    
    def actually_open(self, **task_kwds): 
        debug(f'opening batch {self}...')
        self.opened_at = now()
        self._context_token = CONTEXT.set(self)
        self._batch_token = BATCH.set(self)
        return self
    
    def close(self): 
        as_task(self.actually_close, task_type='XCLOSE', capture=False)()
        # have to take out of context AFTER execution...
        if self._context_token: 
            CONTEXT.reset(self._context_token)
            BATCH.reset(self._batch_token)
    
    def actually_close(self, **task_kwds):
        debug(f'closing batch {self}...')
        self.closed_at = now()

    def error(self): ...

    def add_done_task(self, task: Task): 
        with self._lock: 
            self.done_tasks.append(task)

    def slim_info(self): 
        return {
            'batch_id': self.batch_id, 
            'status': self.infer_status(),
            'opened_at': self.opened_at, 
            'closed_at': self.closed_at,
            'tbls': ', '.join(self.tbls),
            'done_tasks': tuple(tsk.slim_info() for tsk in self.done_tasks),
        }
    
    def infer_status(self): 
        for task in self.done_tasks: 
            if task.status != 'OK': 
                return 'ERR'
        return 'OK'
    

class DefaultBatch(Batch): ...
DEFAULT_BATCH = DefaultBatch(DEFAULT_SESSION)
BATCH.set(DEFAULT_BATCH)


def import_runtime_from_config(setting_name: str, default: type) -> type: 
    '''
    Check a setting name in the global config for the runtime Anchovies
    class to import.
    '''
    setting_name = setting_name.lower()
    import_path = get_config(setting_name)
    if import_path: 
        return anchovies_import(import_path)
    return default


class runtime:
    '''A magic that allows for dynamic imports of classes at runtime.
    
    Your code should inherit from the models in the `anchovies.sdk` pacakage, 
    but use the `runtime()` to initialize new instances of them.
    '''
    def __init__(self):
        self.Tbl: type[Session] = import_runtime_from_config('tbl_cls', Tbl)
        self.Session: type[Session] = import_runtime_from_config('session_cls', Session)
        self.Batch: type[Batch] = import_runtime_from_config('batch_cls', Batch)
        self.Checkpoint: type[Checkpoint] = import_runtime_from_config('checkpoint_cls', CachedCheckpoint)
        self.TaskStore: type[TaskStore] = import_runtime_from_config('task_store_cls', TaskStore)
        self.Task: type[Task] = import_runtime_from_config('task_cls', Task)
        self.TblStore: type[TblStore] = import_runtime_from_config('tbl_store_cls', TblStore)
        self.ResultStore: type[ResultStore] = import_runtime_from_config('result_store_cls', ResultStore)
        self.Cache: type[Cache] = import_runtime_from_config('cache', Cache)
        from anchovies.plugins.core.io import DefaultDataBuffer
        self.DataBuffer: type[DefaultDataBuffer] = import_runtime_from_config('data_buffer_cls', DefaultDataBuffer)
        # debug(self)

    def __str__(self):
        classes = list()
        for _, cls in inspect.getmembers(self): 
            if _.startswith('_'): 
                continue
            classes.append(cast(type, cls).__qualname__)
        return str(tuple(classes))


class AnchoviesEncoder(json.JSONEncoder): 
    def default(self, o):
        if isinstance(o, datetime): 
            return o.isoformat()
        if isinstance(o, Tbl): 
            return str(o)
        return super().default(o)
