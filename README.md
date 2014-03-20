dsa-mq
======

Common libraries for pub/sub messaging in debian

sample code is included under samples/

Scripts:
```
bin/logger.py: submit messages
```

Code excerpts:

Sample usage for a consumer:

```python
import logging
from dsa_mq.connection import Connection

FORMAT = "%(asctime)-15s %(message)s"
logging.basicConfig(format=FORMAT, level=logging.DEBUG)

LOG = logging.getLogger(__name__)

def my_callback(msg):
    pass

conf = {
    'rabbit_userid': 'my_test_user',
    'rabbit_password': 'XXXX',
    'rabbit_virtual_host': 'vhost',
    'rabbit_hosts': ['pubsub02.debian.org', 'pubsub01.debian.org'],
    'use_ssl': False
}

conn = Connection(conf=conf)
conn.declare_topic_consumer('dsa.git.mail',
                            callback=my_callback,
                            queue_name='my_queue',
                            exchange_name='dsa',
                            ack_on_error=False)

try:
    conn.consume()
finally:
    conn.close()
```


Sample usage for a publisher:

```python
import logging
from dsa_mq.connection import Connection

FORMAT = "%(asctime)-15s %(message)s"
logging.basicConfig(format=FORMAT, level=logging.DEBUG)

LOG = logging.getLogger(__name__)

conf = {
    'rabbit_userid': 'my_test_user',
    'rabbit_password': 'XXXX',
    'rabbit_virtual_host': 'vhost',
    'rabbit_hosts': ['pubsub02.debian.org', 'pubsub01.debian.org'],
    'use_ssl': False
}

msg = {
    'newhead': 'HEAD',
    'updated': time.time()
}

conn = Connection(conf=conf)
try:
    conn.topic_send('dsa.git.mail', msg, exchange_name='dsa', timeout=5)
except Exception, e:
    LOG.error("Error sending: %s" % e)
finally:
    conn.close()
```

