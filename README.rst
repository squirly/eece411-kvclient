EECE 411 - Key Value Client
===========================

.. image:: https://travis-ci.org/squirly/eece411-kvclient.png
    :target: https://travis-ci.org/squirly/eece411-kvclient

Installation
------------

Depending on your platform, you may need gcc or another c compiler install in order to compile gevent.
On Ubuntu installing python-dev is sufficient.

To install, simply:

.. code-block:: bash

    pip install git+git://github.com/squirly/eece411-kvclient.git

Usage
-----
To use the basic Key Value client:

.. code-block:: python

    from kvclient import KeyValueClient, InvalidKeyError

    SERVER = 'squirly.ca:9090'
    KEY = 'my_key'
    VALUE = 'the value to be saved'

    client = KeyValueClient(SERVER)
    client.put(KEY, VALUE)
    print(client.get(KEY))
    client.delete(KEY)
    try:
        client.get(KEY)
    except InvalidKeyError, error:
        print(str(error))

To run tests:

.. code-block:: python

    from testing.test_types import get_test_from_string
    from pprint import pprint

    SERVER = 'squirly.ca:9090'
    TEST_NAME = 'simple_compliance'

    test = get_test_from_string(TEST_NAME, [SERVER])

    results = test.run()

    for result in results:
        pprint(result.to_dict())

Development
-----------

Getting the code:

.. code-block:: bash

    git clone git://github.com/squirly/eece411-kvclient.git
    cd eece411-kvclient

To run the tests:

.. code-block:: bash

    python setup.py test

To contribute changes, make a pull request on Github.
