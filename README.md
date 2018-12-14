# os-dbnetget

[![Build Status](https://www.travis-ci.org/cfhamlet/os-dbnetget.svg?branch=master)](https://www.travis-ci.org/cfhamlet/os-dbnetget)
[![codecov](https://codecov.io/gh/cfhamlet/os-dbnetget/branch/master/graph/badge.svg)](https://codecov.io/gh/cfhamlet/os-dbnetget)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/os-dbnetget.svg)](https://pypi.python.org/pypi/os-dbnetget)
[![PyPI](https://img.shields.io/pypi/v/os-dbnetget.svg)](https://pypi.python.org/pypi/os-dbnetget)

Library and command line tool not just for qdb.

This project implement a python qdb toolkit as dbnetget at first. With the progress of development, it is separated into multiple sub-projects as [os-qdb-protocal](https://github.com/cfhamlet/os-qdb-protocal), [os-m3-engine](https://github.com/cfhamlet/os-m3-engine), this project become a framework for similar works not just for qdb.

The main components are client libraries and command line tool.

Client is used for network processing(connect, close, send, receive, etc.) . It is protocol-independent and transparent to user. By now,  there are sync/tornado-async clients, thread-safe client pool and tornado-async client pool are also convenient.

Command line tool's features can be extended by installing extra packages. See [Install](#install).



Greatly appreciate [**Bear Tian**](http://i.youku.com/i/UMTk2ODI0MjI0) and his dbnetget!


# Install

* install package
  ```
  pip install os-dbnetget
  ```

* install extra packages

  | subpackage | install command | enables |
  | :--------- | :-------------- | :------ |
  |m3            | ``pip install os-dbnetget[m3]`` | Install [m3](https://github.com/cfhamlet/os-m3-engine) for command line tool support m3(multi-thread) engine |
  | tornado | ``pip install os-dbnetget[tornado]`` | Install [Tornado](https://github.com/tornadoweb/tornado) for async client and command line tool support tornado engine |
  | rotate | ``pip install os-dbnetget[rotate]`` | Enable write data to rotate file |


# Client API

There are sync/async clients, generally speaking, you should not use them directly. The pool may be your first choice. 

## SyncClientPool

* native multi-thread, do not need extra packages
* thread safe
* retry when network error
* support multi connections with one endpoint



Example:

```python
from os_qdb_protocal import create_protocal
from os_dbnetget.commands.qdb import qdb_key
from os_dbnetget.clients.sync_client import SyncClientPool

endpoints = ['host%02d:8012' % i for i in range(1, 10)]
pool = SyncClientPool(endpoints)

proto = create_protocal('test', qdb_key(b'test-key'))
result = pool.execute(proto)

pool.close()
```





## TornadoClientPool

* support tornado async
* retry when network error
* support multi connections with one endpoint



Example:

```python
from tornado import gen
from tornado.ioLoop import IOLoop

from os_qdb_protocal import create_protocal
from os_dbnetget.commands.qdb import qdb_key
from os_dbnetget.clients.tonado_client import TornadoClientPool

@gen.coroutine
def main():
     
    endpoints = ['host%02d:8012' % i for i in range(1, 10)]
    pool = TornadoClientPool(endpoints)

    proto = create_protocal('test', qdb_key(b'test-key'))
    result = yield pool.execute(proto)

    yield pool.close()

IOLoop.current().run_sync(main)
```





# Command line

* command line tool is on progress, the supported sub-commands:

  ```
  os-dbnetget -h
  ```

* each sub-command has is own features

  ```
  os-dbnetget [sub-command] -h
  ```
  
* some extra packages can be installed for enhancement, see [Install](#install)

  for example, you can install m3 engine to improve processing capacity

  ```
  pip install os-dbnetget[m3]
  ```

  ```
  cat data.txt | os-dbnetget test --engine m3 --thread-num 50 -L endpoints.lst
  ```




# Unit Tests

`$ tox`

# License

MIT licensed.
