import os 
import dateutil
from pathlib import Path
from datetime import datetime
from anchovies.sdk import *


@Datastore.register
class FilesystemDatastore(Datastore): 
    def __init__(self, path: str = None, **kwds):
        super().__init__(path, **kwds)
        self.root_dir_abs = Path(self.root_dir).expanduser().absolute()

    @staticmethod
    def is_compatible(path):
        if ':' in path:
            return False
        try:
            os.makedirs(Path(path).expanduser().absolute(), exist_ok=True)
            return True
        except Exception: 
            return False
        
    @property
    def root_dir(self):
        return (self.origpath or HOME).strip('/')
    
    def aspath(self, path): 
        return Path(self.root_dir_abs, path)
  
    def openb(self, path, mode, **kwds):
        mode = mode or 'r'
        mode += 'b+'
        path = self.aspath(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        return open(path, mode, **kwds)

    def list_files(self, relpathglob=None, *, after=None, before=None):
        import glob
        stream = glob.iglob(str(self.aspath(relpathglob)), include_hidden=True)
        if after: 
            stream = filter(lambda path: self.st_mtime(path) >= after, stream)
        if before: 
            stream = filter(lambda path: self.st_mtime(path) < before, stream)
        prefix = str(self.root_dir_abs) + '/'
        stream = map(lambda p: p.removeprefix(prefix), stream)
        yield from stream

    def st_mtime(self, path): 
        x = datetime.fromtimestamp(
            os.stat(path).st_mtime, 
            dateutil.tz.tzlocal()
        )
        return x

    def describe(self, path) -> FileInfo:
        stat = os.stat(self.aspath(path))
        return FileInfo(
            path=path,
            modified_at=datetime.fromtimestamp(
                stat.st_mtime, 
                dateutil.tz.tzlocal(),
            ),
            size=stat.st_size,
        )

    def delete(self, path):
        try: 
            os.remove(path)
        except FileNotFoundError: 
            pass
