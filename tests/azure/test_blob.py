import tempfile
from pytest import fixture
from unittest.mock import patch
from anchovies.sdk import Datastore
from anchovies.plugins.core.azure.blob import BlobDatastore


@fixture
def datastore(session, newmeta): 
    with patch('anchovies.plugins.core.azure.blob.BlobServiceClient') as mock, \
            tempfile.TemporaryFile('wb+', buffering=0) as buf:
        download_mock = (
            mock.return_value
                .get_container_client.return_value
                .get_blob_client.return_value
                .download_blob
        )
        buf.write(b'Hello!')
        buf.seek(0)
        download_mock.return_value = buf
        # TODO: unsure if this will suffice to mock the Blob return value
        db = BlobDatastore('az://container/blob')
        db._mock_blob_client = (
            mock.return_value
                .get_container_client.return_value
                .get_blob_client
        )
        db._mock_blob_uploader = (
            mock.return_value
                .get_container_client.return_value
                .get_blob_client.return_value
                .upload_blob
        )
        yield db


def test_read(datastore): 
    with datastore.open('file.txt') as f: 
        data = f.read()
    assert data == 'Hello!'
    datastore._mock_blob_client.assert_called_with('blob/file.txt')


def test_write(datastore): 
    with datastore.open('file.txt', 'w') as f:
        written = f.write('Hello!')
    assert written
    datastore._mock_blob_uploader.assert_called_once()
