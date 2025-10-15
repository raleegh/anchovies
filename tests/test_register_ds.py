from anchovies.sdk import Datastore, FilesystemDatastore, HOME


def test_new(): 
    ds = Datastore.new()
    ds = Datastore.new(HOME)
    assert isinstance(ds, FilesystemDatastore)

