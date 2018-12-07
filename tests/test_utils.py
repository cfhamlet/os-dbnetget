from os_dbnetget import utils


def test_split_endpoint():
    endpoint = 'test01:8080'
    s, p = utils.split_endpoint(endpoint)
    assert s == 'test01'
    assert p == 8080
