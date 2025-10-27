import os 
import dateutil
import logging
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from anchovies.sdk import *


logger = logging.getLogger(__name__)
debug = logger.debug


@Datastore.register
class FilesystemDatastore(Datastore): 
    def __init__(self, path: str = None, **kwds):
        super().__init__(path, **kwds)
        self.root_dir_abs = Path(self.root_dir) #.expanduser().absolute()
        if str(self.root_dir_abs).startswith('~'): 
            self.root_dir_abs = self.root_dir_abs.expanduser() #.absolute()
        debug('FilesystemDatastore original %s' % self.original_path)
        debug('FilesystemDatastore root %s' % self.root_dir_abs)

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
        return (self.origpath or HOME).removesuffix('/')
    
    def aspath(self, path): 
        return Path(self.root_dir_abs, path)
  
    def openb(self, path, mode, **kwds):
        mode = mode or 'r'
        mode += 'b+'
        path = self.aspath(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if 'w' in mode: 
            return self._openb_write(path, **kwds)
        return open(path, mode, **kwds)
    
    def _openb_write(self, path, **kwds): 
        # i was having trouble with corrupted GZIP files
        # so send data to spool and then write
        # that way if the garbage collector is acting up
        # or doesnt run, no corrupted file
        def callback(stream):
            stream.flush()
            stream.seek(0) 
            with open(path, 'wb') as fdst:
                shutil.copyfileobj(stream, fdst)
        writer = CallbackTempfile(mode='wb+', callback=callback)
        return writer

    def list_files(self, relpathglob=None, *, after=None, before=None):
        import glob
        path = str(self.aspath(relpathglob))
        stream = glob.iglob(path, include_hidden=True)
        if after: 
            stream = filter(lambda path: self.st_mtime(path) >= after, stream)
        if before: 
            stream = filter(lambda path: self.st_mtime(path) < before, stream)
        prefix = str(self.root_dir_abs) + '/'
        stream = map(lambda p: p.removeprefix(prefix), stream)
        yield from sorted(stream)

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
            os.remove(self.aspath(path))
        except FileNotFoundError: 
            pass


class CallbackTempfile(tempfile.SpooledTemporaryFile): 
    def __init__(self, *args, callback=None, **kwargs): 
        super().__init__(*args, **kwargs)
        self.callback = callback
    
    def close(self): 
        if self.callback is not None: 
            self.flush()
            self.callback(self)
        super().close()
