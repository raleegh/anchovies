def test_session_adds_result(session, newmeta): 
    with session: 
        ...
    path = newmeta.anchovy_home('$results', '*')
    files = tuple(newmeta.list_files(path))
    assert files
    assert newmeta.anchovy_home('$results', 'anchovy_run_results.json') \
        in files
