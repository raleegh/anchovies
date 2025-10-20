from unittest.mock import patch
from anchovies.sdk import Datastore, HOME
from anchovies.plugins.core.filesystem import FilesystemDatastore
from anchovies.plugins.core.azure.blob import BlobDatastore


def test_new(): 
    ds = Datastore.new(HOME)
    assert isinstance(ds, FilesystemDatastore)


@patch('anchovies.plugins.core.azure.blob.BlobServiceClient')
def test_blob(mocked_client):
    ds = Datastore.new('az://some_container/some_prefix/more')
    assert isinstance(ds, BlobDatastore)
