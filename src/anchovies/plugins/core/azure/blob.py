import os
import io
import fnmatch
import tempfile
from urllib.parse import urlparse
from typing import cast
try:
    from azure.storage.blob import BlobServiceClient
    installed = True
except ImportError: 
    installed = False
from anchovies.sdk import Datastore, FileInfo


AZURE_BLOB_ACCOUNT_URL = os.getenv('AZURE_BLOB_ACCOUNT_URL')
AZURE_BLOB_CREDENTIAL = os.getenv('AZURE_BLOB_CREDENTIAL')


class BlobDatastore(Datastore): 
    def __init__(self, path, **kwds):
        super().__init__(path, **kwds)
        parsed = urlparse(path)
        self.account_url = None
        self.container_name = parsed.hostname
        self.prefix = parsed.path.strip('/')
        if self.container_name.endswith('.blob.core.windows.net'): 
            self.account_url = 'https://' + parsed.hostname
            self.container_name = parsed.path.split('/')[1]
            self.prefix = '/'.joins(parsed.path.split('/')[1:])
        self.account_url = self.account_url or AZURE_BLOB_ACCOUNT_URL
        assert self.account_url, \
            'The account url must be provided for Azure Blob ' \
            'either via the datastore connection string or via ' \
            'the env variable "AZURE_BLOB_ACCOUNT_URL".'
        self.credential = AZURE_BLOB_CREDENTIAL
        assert AZURE_BLOB_CREDENTIAL, \
            'The "AZURE_BLOB_CREDENTIAL" environment variable is ' \
            'required when using the BlobDatastore.'
        self.client = BlobServiceClient(
            account_url=self.account_url, 
            credential=self.credential,
        )
        self.container = self.client.get_container_client(self.container_name)

    @staticmethod
    def is_compatible(path):
        return path.startswith('az://')

    def delete(self, path):
        self.container.delete_blob(self.qualified(path))

    def describe(self, relpath):
        info = self.container.get_blob_client(self.qualified(relpath)).get_blob_properties()
        return FileInfo(
            path=relpath,
            modified_at=info.last_modified, 
            size=info.size,
        )
        # TODO: cache...

    def list_files(self, relpathglob=None, *, after=None, before=None):
        relpathglob = relpathglob or ''
        start_path = relpathglob.split('*')
        stream = self.container.list_blobs(self.qualified(start_path))
        if after: 
            stream = filter(lambda b: b.last_modified >= after, stream)
        if before: 
            stream = filter(lambda b: b.last_modified < before, stream)
        stream = map(lambda b: self.unqualified(b.name), stream)  #TODO: check
        stream = filter(lambda p: fnmatch.fnmatch(p, relpathglob), stream)
        yield from stream

    def openb(self, path, mode = None, **kwds):
        blob = self.container.get_blob_client(self.qualified(path))
        if mode == 'r':
            # stream = blob.download_blob()
            # stream = io.BufferedReader(stream)
            # return stream
            return blob.download_blob()
        if mode == 'w': 
            def callback(stream: io.BufferedRandom):
                stream.flush()
                stream.seek(0) 
                blob.upload_blob(stream)
            writer = CallbackTempfile(mode='wb+', callback=callback)
            return cast(io.BufferedWriter, writer)

    def qualified(self, unqualified_path) -> str: 
        '''Append the prefix specified for the datastore connection.'''
        return self.prefix + '/' + unqualified_path
    
    def unqualified(self, qualified_path: str) -> str: 
        '''Remove the prefix specified for the datastore connection.'''
        return qualified_path.removeprefix('/').removeprefix(self.prefix)


class CallbackTempfile(tempfile.SpooledTemporaryFile): 
    def __init__(self, *args, callback=None, **kwargs): 
        super().__init__(*args, **kwargs)
        self.callback = callback
    
    def close(self): 
        if self.callback is not None: 
            self.flush()
            self.callback(self)
        super().close()


if installed: 
    Datastore.register(BlobDatastore)
