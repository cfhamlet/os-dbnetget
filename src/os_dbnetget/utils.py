
def split_endpoint(endpint):
    address, port = endpint.split(':')
    port = int(port)
    return address, port
