dsa-mq
======

Common libraries for pub/sub messaging in debian

Sample usage for a consumer:

```
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
conn.declare_fanout_consumer(queue='my_queue', callback=my_callback)
conn.consume()
```


Sample usage for a publisher:

```
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
    conn.fanout_send('my_exchange', msg)
except Exception, e:
    LOG.error("Error sending: %s" % e)
finally:
    conn.close()
```
