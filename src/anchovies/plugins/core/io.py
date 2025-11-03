import uuid
import logging; logger = logging.getLogger(__name__)
import concurrent.futures as cf
import atexit
import orjson as json
import isal.igzip_threaded as gzip
import io
from abc import ABC, abstractmethod
from contextlib import ExitStack
from datetime import datetime, date, timedelta, UTC
from pytz import timezone
from typing import Iterator
from anchovies.sdk import Datastore, Tbl, Task, now, context, cached, get_config


def not_naive_ts(dt: datetime): 
    if dt.tzinfo is None: 
        dt = timezone('UTC').localize(dt)
    return dt 


pool = cf.ThreadPoolExecutor()
atexit.register(pool.shutdown)


class BaseDataBuffer(ABC):
    '''
    A buffer over files in a datalake-like datastore.

    Shares some similarity to `io.BufferedIOBase`, but ultimately
    this is it's own set of classes with only _similar_ behavior.

    Ideally, we are working over a set of files like: 
    ```
    tbl/
        file1.json  mod: 2025-01-01
        file2.json  mod: 2025-01-02
    ```
    And therefore, can use `seek(datetime)` to set an anchor 
    within the "stream".
    '''
    def __init__(self):
        super().__init__()
        self._start = self.position = now()

    @abstractmethod
    def seekable(self): 
        '''Check if the stream is seekable.'''
        ...

    def seek(self, offset: datetime | timedelta | int, whence: datetime=None) -> None: 
        '''Provide offset (and whence) to adjust the stream position.'''
        assert self.seekable()
        if isinstance(offset, int): 
            assert offset in {0, -1}, 'Can only provide offsets (0, -1).'
            if offset == -1: 
                return self.seek_end()
            return self.seek_start()
        if whence: 
            assert isinstance(offset, timedelta), \
                'When using "whence", "offset" must be a timedelta.'
            self.position = not_naive_ts(whence)
        if isinstance(offset, datetime): 
            self.position = not_naive_ts(offset)
            return
        self.position += offset

    @abstractmethod
    def seek_start(self): 
        '''When the user requests "START" of stream with position-0, 
        navigate to the first datetime.
        '''
        ...

    @abstractmethod
    def seek_end(self): 
        '''When the user requests "START" of stream with position-0, 
        navigate to the first datetime.
        '''
        ...
    
    def tell(self): 
        '''Reveal the stream position.'''
        return self.position
    
    @abstractmethod
    def pathable(self) -> bool: 
        '''Check if the stream can iterate paths in the underlying files.'''
        ...

    def readpaths(self, hint = -1) -> Iterator[str]: 
        '''Iterate paths from the current position.'''
        if not self.pathable(): 
            raise NotImplementedError(f'{self} does not implement path iteration.')
    
    @abstractmethod
    def readable(self): 
        '''Check if the stream can read dictionaries.'''
        ...

    def read(self, hint = -1) -> Iterator[dict]: 
        '''Read dictionaries from the current position.'''
        if not self.readable(): 
            raise NotImplementedError(f'{self} does not implement dict iteration.')

    @abstractmethod
    def writable(self): 
        '''Check if the stream can write (via append ONLY).'''
        ...

    def write(self, line: dict) -> int: 
        '''Write a single dict to the underlying stream in datastore.'''
        if not self.writable(): 
            raise NotImplementedError(f'{self} does not support write.')

    def writelines(self, lines: Iterator[dict]) -> int: 
        '''Write a series of dictionaries to the buffer.'''
        return sum(self.write(line) for line in lines)
    
    def close(self): 
        '''Close the buffer.'''
        ...

    def __iter__(self): 
        return self.read()
    
    def __enter__(self): 
        return self

    def __exit__(self, *args): 
        self.close()


class FileDataBuffer(BaseDataBuffer): 
    def __init__(self, path: str):
        super().__init__()
        self._path = path
        self._current_path = None

    def pathable(self):
        return True
    
    def tellpath(self): 
        return self._current_path or self._path
    

class GzipJsonBuffer(FileDataBuffer): 
    '''Read/write a gziped-JSON file.'''
    def __init__(self, path, *, datastore: Datastore=None):
        super().__init__(path)
        self.datastore = self.db = datastore or context().datastore
        self._writeable, self._readable = False, False 
        self._compression_factor = 0.2
        self._buf = None
        self._stack = ExitStack()

    def seekable(self):
        return False
    
    def seek_start(self):
        raise NotImplementedError('Not supported. Stream not seekable.')

    def seek_end(self):
        raise NotImplementedError('Not supported. Stream not seekable.')

    def write(self, line):
        self.check_write()
        raw = json.dumps(line) + b'\n'
        self._buf.write(raw)
        return round(len(raw) * self._compression_factor)  
        # approximation of write size
    
    def writable(self):
        return self._writeable
    
    def check_write(self): 
        assert not self._readable
        if not self._buf: 
            self.make_buffer()
            self._writeable = True

    def make_buffer(self): 
        buf = self.db.open(self._path, 'wb')
        buf = gzip.open(buf, 'wb', threads=4)
        self._buf = self._stack.enter_context(buf)

    def __exit__(self, *args):
        if self.writable():
            self._buf.flush()
        self._stack.close()
        return super().__exit__(*args)

    def read(self, hint = -1):
        self.check_read()
        logger.debug(f'read from {self._path}')
        with (
                self.db.open(self._path, 'rb') as buf,
                gzip.open(buf, 'rb') as buf,
                io.TextIOWrapper(buf, encoding='utf-8') as buf
        ): 
            for line in buf.readlines(): 
                yield json.loads(line)

    def readable(self):
        return self._readable

    def check_read(self): 
        assert not self._writeable
        self._readable = True
        

class TypedJsonBuffer(GzipJsonBuffer): 
    '''
    Add an attribute "_types" to annotate complex types.
    
    Type annotations:
    * D: date
    * DT: datetime
    * B: binary (from hex)
    '''
    def write(self, line):
        line = self.to_annotated(line)
        return super().write(line)
    
    def read(self, hint=-1):
        return map(self.from_annotated, super().read(hint))
    
    def to_annotated(self, line: dict) -> dict: 
        '''Add the "_types" attribute to re-serialize later.'''
        line = {k: v for k, v in line.items()}
        types = dict()
        changed = dict()
        for k, v in line.items(): 
            if isinstance(v, date): 
                types[k] = 'D'
                changed[k] = date.isoformat(v)
            if isinstance(v, datetime): 
                types[k] = 'DT'
                changed[k] = datetime.isoformat(v)
            if isinstance(v, bytes): 
                types[k] = 'B'
                changed[k] = bytes.hex(v)
        line.update(**changed)
        line['_types'] = self.make_annotation(types)
        return line

    def from_annotated(self, line: dict) -> dict: 
        '''Check the "_types" attribute to serialize.'''
        types = tuple(self.break_annotation(line.pop('_types', '')))
        if not types: 
            return line
        for col, typ in types: 
            if typ == 'D': 
                line[col] = date.fromisoformat(line[col])
            if typ == 'DT': 
                line[col] = datetime.fromisoformat(line[col])
            if typ == 'B': 
                line[col] = bytes.fromhex(line[col])
        return line
                
    @staticmethod
    def make_annotation(t): 
        return ' '.join('@'.join((k, v)) for k, v in t.items())

    @staticmethod
    def break_annotation(t): 
        notes = tuple(s.split('@') for s in t.split(' '))
        for note in notes:
            if len(note) == 2: 
                yield note


class NaivePathBuffer(FileDataBuffer): 
    '''Seek over Gzip'd JSON files in the `$anchovy/data/$tbl` path.'''
    def __init__(self, path, /, anchovy_id: str=None, datastore: Datastore=None):
        self.tbl = None
        if isinstance(path, Tbl): 
            self.tbl = path
        super().__init__(path)
        self.datastore = self.db = datastore or context().datastore
        if isinstance(path, Tbl): 
            anchovy_id = anchovy_id or path.anchovy_id
        self.anchovy_id = anchovy_id or context().anchovy_id
        self.desired_file_size = 1024 * 1024 * 6  # 6MB
        # it's BEYOND me, but this calc is WAY off
        self._buffer_cls = TypedJsonBuffer
        self._buf = None
        self._stack = ExitStack()
        self.__futures = set()

    def __hash__(self):
        return hash((
            self.anchovy_id, 
            hash(self._path), 
            self.datastore.original_path
        ))

    def seekable(self):
        return True 
    
    def seek_start(self):
        self.position = datetime(2001, 1, 1, tzinfo=UTC)
        for path in self.readpaths(1): 
            break

    def seek_end(self): 
        self.position = datetime(2001, 1, 1, tzinfo=UTC)
        for path in self.readpaths(-1): 
            ...
    
    def make_data_path(self, extra: str=None): 
        '''Build the Anchovies data path.

        Example: `$anchovies/$user/$id/$tbl/data/{{extra}}*`
        '''
        if self.tbl and 'path' in self.tbl.properties: 
            return self.make_custom_path(extra)
        path = self.db.context_home(
            self.anchovy_id, 
            'data', 
            self._path, 
            (extra or '') + '*'
        )
        return path
    
    def make_custom_path(self, extra: str=None):
        '''Build a data path based on the Tbl's settings.'''
        path = self.tbl.properties['path']
        parts = list(path.split('*'))
        if extra:
            if len(parts) > 1:
                parts.insert(-1, extra)
            else: 
                parts[0] = parts[0] + '/' + extra
        return '*'.join(parts)

    @cached('io.readpaths', ttl_hook=lambda: get_config('io_cache_ttl', 60 * 10, astype=int))
    def readpaths(self, hint = -1):
        def generate():
            total_read = 0
            after = self.tell()
            path = self.make_data_path()
            for path in self.db.list_files(path, after=after): 
                self._current_path = path
                info = self.db.describe(path)
                self.position = info.modified_at
                total_read += info.size
                yield path
                if hint > 0 and total_read > hint: 
                    logger.debug(
                        f'Hit read size limit @ {total_read:,}'
                        f' bytes (limit={hint:,})'
                    )
                    return
        return tuple(generate())
    
    def read(self, hint=-1):
        for path in self.readpaths(hint): 
            self.position = self.db.describe(path).modified_at
            self._current_path = path
            self.make_buffer(path)
            yield from self._buf

    def readable(self):
        return self._buf.readable()

    def write(self, line):
        self.maybe_make_buffer()
        written = self._buf.write(line)
        self._written_bytes += written
        self.maybe_flush()
        return written

    def writable(self):
        return self._buf.writable()

    def make_buffer(self, path: str=None): 
        path = path or self.new_path()
        self._buf = self._buffer_cls(path, datastore=self.db)
        self._buf = self._stack.enter_context(self._buf)
        self._written_bytes = 0

    def maybe_make_buffer(self): 
        if getattr(self, '_buf', None) is None: 
            self.make_buffer()

    def full(self): 
        return self._written_bytes > self.desired_file_size
    
    def flush(self): 
        self.promise_futures()
        stack, self._buf = self._stack, None 
        self.__futures.add(pool.submit(stack.close))
        self._stack = ExitStack()

    def maybe_flush(self): 
        if self.full(): 
            self.flush()

    def new_path(self):
        ts = now().strftime('%Y%m%d%H%M%S')
        rand = str(uuid.uuid4())[:3]
        return self.db.context_home(self.anchovy_id, 'data', str(self._path), f'{ts}.{rand}.json.gz')

    def __exit__(self, *args):
        self.flush()
        self.promise_futures()
        return super().__exit__(*args)
        # TODO: would like to add threading to do background tasks & concurrency
        # TODO: during an error, leaves a bad file

    def promise_futures(self):
        for fut in cf.as_completed(self.__futures): 
            fut.result()
            self.__futures.remove(fut)


class DatetimePathBuffer(NaivePathBuffer): 
    '''Seek over Gzip'd JSON files in the `$anchovy/data/$tbl/YYYmmdd*` path.
    
    Because the path must have the day in the prefix, we can more intelligently 
    scan over files associated to the buffer.
    '''   
    def seek_start(self):
        self.position = datetime(2001, 1, 1, tzinfo=UTC)
        for path in super().readpaths(1): 
            break

    def seek_end(self): 
        self.position = now()
        for path in super().readpaths(0): 
            ...

    @cached('io.readpaths', ttl_hook=lambda: get_config('io_cache_ttl', 60 * 10, astype=int))
    def readpaths(self, hint = -1):
        def generate():
            total_read = 0
            until = now() + timedelta(days=1) # easiest way to make this work as expected
            today = after = self.tell()
            while today <= until: 
                date_part = today.strftime('%Y%m%d')
                path = self.make_data_path(date_part)
                for path in self.db.list_files(path, after=after): 
                    self._current_path = path
                    info = self.db.describe(path)
                    self.position = info.modified_at
                    total_read += info.size
                    yield path
                    if hint > 0 and total_read > hint: 
                        return
                today += timedelta(days=1)
        return tuple(generate())


class StatisticsBuffer(DatetimePathBuffer): 
    '''Collect stream statistics as write activities take place.'''
    def __init__(self, path, /, anchovy_id = None, datastore = None):
        super().__init__(path, anchovy_id, datastore)
        self.task = None
        self._stats_written_bytes = 0
        self._stats_row_count = 0
        self._stats_data_change_timestamp_field = None
        self._stats_last_data_change_timestamp = None
        if self.tbl: 
            if self.tbl.data_change_timestamp_field: 
                self._stats_data_change_timestamp_field = \
                    self.tbl.data_change_timestamp_field
                self.check_data_change_timestamp = self._check_dcts
        self._timezone = timezone(get_config('data_timezone', 'UTC'))

    def attach_task(self, task: Task): 
        '''Associate an anchovies Task object for monitoring.'''
        self.task = task

    def has_task(self): 
        '''Check if a task was associated.'''
        return self.task is not None
    
    def write(self, line):
        b = super().write(line)
        self.check_data_change_timestamp(line)
        self._stats_written_bytes += b
        self._stats_row_count += 1
        return b 
    
    def check_data_change_timestamp(self, line: dict): 
        '''Check if the row has a data change timestamp.
        
        If it does, save the value if it is larger than the last.
        '''
        pass

    def _check_dcts(self, line: dict): 
        val: datetime = line.get(self._stats_data_change_timestamp_field)
        if val: 
            if val.tzinfo is None: 
                val = self._timezone.localize(val)
            if self._stats_last_data_change_timestamp: 
                val = max(val, self._stats_last_data_change_timestamp)
            self._stats_last_data_change_timestamp = val

    def close(self): 
        super().close()
        if not self.has_task(): 
            return
        lag = None
        if self._stats_last_data_change_timestamp:
            lag = round(
                (
                    datetime.now(UTC)
                        - self._stats_last_data_change_timestamp
                ).total_seconds()
            )
        self.task.with_(
            size=self._stats_written_bytes,
            rows=self._stats_row_count,
            last_data_change_timestamp=self._stats_last_data_change_timestamp,
            lag=lag,
        )
        logger.debug(
            f'buffer statistics for {self.tbl} '
            f'size={self._stats_written_bytes:,} '
            f'rows={self._stats_row_count:,} '
            f'data change date={self._stats_last_data_change_timestamp} '
            f'lag (seconds)={lag or 0:,}'
        )
        


# save for export
DefaultDataBuffer = StatisticsBuffer
