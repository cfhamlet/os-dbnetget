from os_dbnetget.clients.tornado_client import TornadoClient


def test_tornado_client_wrong_endpoint():
    c = TornadoClient('unknowhost.com', 8012)
    # TODO
