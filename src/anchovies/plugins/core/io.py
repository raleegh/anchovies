import json
import gzip
import uuid
from abc import ABC, abstractmethod
from contextlib import ExitStack
from datetime import datetime, date, timedelta
from typing import Iterator
from anchovies.sdk import Datastore, Tbl, now, context, cached, get_config


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

    def seek(self, offset: datetime | timedelta, whence: datetime=None) -> None: 
        '''Provide offset (and whence) to adjust the stream position.'''
        assert self.seekable()
        if whence: 
            assert isinstance(offset, timedelta), \
                'When using "whence", "offset" must be a timedelta.'
            self.position = whence
        if isinstance(offset, datetime): 
            self.position = offset
            return
        self.position += offset
    
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
    
    def __iter__(self): 
        return self.read()
    
    def __enter__(self): 
        return self

    def __exit__(self, *args): 
        pass


class FileDataBuffer(BaseDataBuffer): 
    def __init__(self, path: str):
        super().__init__()
        self._path = path

    def pathable(self):
        return True
    
    def tellpath(self): 
        return self._path
    

class GzipJsonBuffer(FileDataBuffer): 
    '''Read/write a gziped-JSON file.'''
    def __init__(self, path, *, datastore: Datastore=None):
        super().__init__(path)
        self.datastore = self.db = datastore or context().datastore
        self._writeable, self._readable = False, False 
        self._compression_factor = 0.20
        self._buf = None
        self._stack = ExitStack()

    def seekable(self):
        return False

    def write(self, line):
        self.check_write()
        json.dump(line, self._buf)
        self._buf.write('\n')
        return round(len(str(line)) * self._compression_factor)  # approximation of write size
    
    def writable(self):
        return self._writeable
    
    def check_write(self): 
        assert not self._readable
        if not self._buf: 
            self.make_buffer()
            self._writeable = True

    def make_buffer(self): 
        buf = self.db.open(self._path, 'wb')
        buf = gzip.open(buf, 'wt')
        self._buf = self._stack.enter_context(buf)

    def __exit__(self, *args):
        if self.writable():
            self._buf.flush()
        self._stack.close()
        return super().__exit__(*args)

    def read(self, hint = -1):
        self.check_read()
        with self.db.open(self._path, 'rb') as buf, gzip.open(buf, 'rt') as buf: 
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
        types = self.break_annotation(line.pop('_types', ''))
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
        yield from (tuple(s.split('@')) for s in t.split(' '))


class DefaultDataBuffer(FileDataBuffer): 
    '''Seek over Gzip'd JSON files in the `$anchovy/data/$tbl` path.'''
    def __init__(self, path, /, anchovy_id: str=None, datastore: Datastore=None):
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

    def __hash__(self):
        return hash((self.anchovy_id, self._path, self.datastore.original_path))

    def seekable(self):
        return True 
    
    @cached('io.readpaths', ttl_hook=lambda: get_config('io_cache_ttl', 60 * 10, astype=int))
    def readpaths(self, hint = -1):
        def generate():
            total_read = 0
            until = now()
            today = after = self.tell()
            while today <= until: 
                date_part = today.strftime('%Y%m%d')
                path = self.db.context_home(
                    self.anchovy_id, 
                    'data', 
                    self._path, 
                    date_part + '*'
                )
                for path in self.db.list_files(path, after=after): 
                    self._path = path
                    info = self.db.describe(path)
                    self._position = info.modified_at
                    total_read += info.size
                    yield path
                    if hint > 1 and total_read > hint: 
                        break
                today += timedelta(days=1)
        return tuple(generate())
    
    def read(self, hint=-1):
        for path in self.readpaths(hint): 
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
        stack, self._buf = self._stack, None 
        stack.close()
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
        return super().__exit__(*args)
        # TODO: would like to add threading to do background tasks & concurrency
        # TODO: during an error, leaves a bad file
