import itertools
from io import IOBase
from datetime import timedelta, datetime
from typing import Literal, Iterator
from anchovies.sdk import context, import_runtime_from_config, Datastore


class PathDatetimeMagic: 
    '''This can convert paths into datetimes!'''
    def __init__(
        self,
        path: str,
        increment: timedelta=None,
    ): 
        self.path = path
        self.increment = increment or timedelta(days=1)

    # def walk(self, after: datetime=None, before: datetime=None) -> Iterator[str]: 
    #     today = after = after or datetime(1900, 1, 1)
    #     while True: 
    #         yield today
    #         today += self.increment
    #         if today > 


class DataBuffer(IOBase): 
    def __init__(
        self, 
        name: str, 
        namespace: str,
        *, 
        db: Datastore=None,
    ): 
        self.name = name
        self.namespace = namespace
        if not db: 
            db = context().metastore.db
        self.db = db 
        self.path_walker = PathDatetimeMagic(self.data_path)

    @classmethod
    def open(
        cls, 
        name: str,
        namespace: str=None,
        mode: Literal['r', 'a']=None,
    ) -> 'DataBuffer': 
        '''
        Create a new Data Buffer against a table.

        Parameters:
        * name (str): table name (unqualified)
        * namespace (str): anchovy name (defaulting to current if none)
        * mode (Literal['r', 'a']): buffering mode
        '''
        mode = mode or 'r'
        return cls(name=name, namespace=namespace)
        # TODO: other args?

    def seek(self, offset: timedelta | datetime, whence: datetime=None):
        ...
    
    def tell(self) -> datetime:
        return None
        
    def readline(self, size = -1) -> dict: ...
    def read(self, max=-1) -> list[dict]: ...
    def writeline(self, line: dict): ...
    def write(self, line: dict): 
        return self.writeline(line)
    
    def readpaths(self, hint = -1) -> Iterator[str]: 
        '''Use a combination of the `PathDatetimeMagic` and 
        `Datastore.list_files()` to yield paths AFTER the current
        position in the stream
        '''
        after = self.tell()
        # # batches = self.path_walker.walk(after=after)
        # # batches = map(lambda p: self.db.list_files(p, after=after), batches)
        # paths = itertools.chain.from_iterable(batches)
        paths = self.db.list_files(self.data_path, after)
        yield from paths
        # TODO: update position
    
    def readlines(self, hint = -1):
        '''An alias for the `readpaths()` method.'''
        return self.readpaths(hint)
    
    def writelines(self, lines):
        raise NotImplementedError('This method has no function (intentionally)')

    def __iter__(self) -> Iterator[dict]: 
        yield from self.read()
