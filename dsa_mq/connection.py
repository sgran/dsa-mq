#    Based on work from the openstack project
#    Openstack work is
#    Copyright 2011 OpenStack Foundation
#    Changes for Debian are
#    Copyright 2013 Stephen Gran <sgran@debian.org>
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import time
import socket
import itertools
import logging

import kombu
import kombu.connection

from dsa_mq.publisher import TopicPublisher
from dsa_mq.consumer import TopicConsumer

LOG = logging.getLogger(__name__)

class RPCException(Exception):

    def __init__(self, message=None, **kwargs):
        if not message:
            message = "An unknown RPC related exception occurred."
        super(RPCException, self).__init__(message)


class Connection(object):
    """
    Connection class.  You are expected to pass a dict of config options
    in the constructor, and then use the object to consume or publish messages


    ################################################################

    Sample usage for a consumer:


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

    ################################################################

    Sample usage for a publisher:


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

    ################################################################

    """

    pool = None

    def __init__(self, conf):
        self.conf = conf
        self.consumers         = []
        self.max_retries       = conf.get('max_retries', 3)
        self.interval_start    = conf.get('interval_start', 1)
        self.interval_stepping = conf.get('interval_stepping', 3)
        self.interval_max      = conf.get('interval_max', 30)
        if self.max_retries <= 0:
            self.max_retries = None

        params_list = []
        port = 5671
        for hostname in self.conf['rabbit_hosts']:
            host_list = hostname.split(':')
            if len(host_list) == 2:
                rabbit_port = host_list[1]
            else:
                rabbit_port = port

            params = {
                'hostname': host_list[0],
                'port': rabbit_port,
                'userid': self.conf['rabbit_userid'],
                'password': self.conf['rabbit_password'],
                'virtual_host': self.conf['rabbit_virtual_host'],
                'heartbeat': self.conf.get('heartbeat', 60),
            }

            params['ssl'] = self._fetch_ssl_params()
            params['login_method'] = self.conf.get('login_method')
            params_list.append(params)

        self.params_list = params_list
        self.connection = None
        self.connection_errors = None
        self.channel = None
        self.reconnect()

    def _fetch_ssl_params(self):
        """Handles fetching what ssl params should be used for the connection
        (if any).
        """
        ssl_params = dict()

        if self.conf['use_ssl']:
            if self.conf.get('ssl_keyfile'):
                ssl_params['keyfile'] = self.conf['ssl_keyfile']
            if self.conf.get('ssl_certfile'):
                ssl_params['certfile'] = self.conf['ssl_certfile']
            if self.conf.get('ssl_ca_certs'):
                ssl_params['ca_certs'] = self.conf['ssl_ca_certs']

        return ssl_params or True

    def _connect(self, params):
        """Connect to rabbit.  Re-establish any queues that may have
        been declared before if we are reconnecting.  Exceptions should
        be handled by the caller.
        """

        if self.connection:
            LOG.info("Reconnecting to AMQP server on "
                       "%(hostname)s:%(port)d" % params)

        self.close()
        self.connection = kombu.connection.BrokerConnection(**params)
        self.connection_errors = self.connection.connection_errors
        self.connection.connect()
        self.channel = self.connection.channel()

        for consumer in self.consumers:
            consumer.reconnect(self.channel)
        LOG.info('Connected to AMQP server on %(hostname)s:%(port)d' % params)

    def reconnect(self):
        """Handles reconnecting and re-establishing queues.
        Will retry up to self.max_retries number of times.
        self.max_retries = 0 means to retry forever.
        Sleep between tries, starting at self.interval_start
        seconds, backing off self.interval_stepping number of seconds
        each attempt.
        """

        attempt = 0
        # If self.max_tries is false-ish, we loop forever.  Otherwise, we
        # only loop for max_retries
        while (not self.max_retries) or (attempt < self.max_retries):
            params = self.params_list[attempt % len(self.params_list)]
            attempt += 1
            try:
                self._connect(params)
                return
            except (IOError, self.connection_errors) as e:
                if str(e) == 'Socket closed':
                    e = 'Socket closed (probably authentication failure)'
                LOG.warning("reconnect: IOerror: %s" % e)
            except Exception as e:
                # NOTE(comstud): Unfortunately it's possible for amqplib
                # to return an error not covered by its transport
                # connection_errors in the case of a timeout waiting for
                # a protocol response.  (See paste link in LP888621)
                # So, we check all exceptions for 'timeout' in them
                # and try to reconnect in this case.
                if 'timeout' not in str(e):
                    LOG.warning(e)

            if attempt == 1:
                sleep_time = self.interval_start or 1
            elif attempt > 1:
                sleep_time += self.interval_stepping
            if self.interval_max:
                sleep_time = min(sleep_time, self.interval_max)

            time.sleep(sleep_time)

        msg = ('Unable to connect to AMQP server on '
                '%s:%d after %d '
                'tries: %s') % (params['hostname'],
                                params['port'],
                                attempt,
                                e)
        raise RPCException(message=msg)

    def ensure(self, error_callback, method, *args, **kwargs):
        while True:
            try:
                return method(*args, **kwargs)
            except (self.connection_errors, socket.timeout, IOError) as e:
                if error_callback:
                    error_callback(e)
            except Exception as e:
                # NOTE(comstud): Unfortunately it's possible for amqplib
                # to return an error not covered by its transport
                # connection_errors in the case of a timeout waiting for
                # a protocol response.  (See paste link in LP888621)
                # So, we check all exceptions for 'timeout' in them
                # and try to reconnect in this case.
                LOG.warning(e)
                if error_callback:
                    error_callback(e)
                if 'timeout' not in str(e):
                    raise RPCException(message=e)
            self.reconnect()

    def close(self):
        """Close/release this connection."""
        if self.connection:
            try:
                self.connection.release()
            except self.connection_errors:
                pass
        self.connection = None

    def reset(self):
        """Reset a connection so it can be used again."""
        self.channel.close()
        self.channel = self.connection.channel()
        self.consumers = []

    def declare_consumer(self, consumer_cls, topic, callback, **kwargs):
        """Create a Consumer using the class that was passed in and
        add it to our list of consumers
        """

        def _connect_error(exc):
            LOG.warning("declare_consumer: %s" % exc)

        def _declare_consumer():
            consumer = consumer_cls(self.conf, self.channel, topic, callback, **kwargs)
            self.consumers.append(consumer)
            return consumer
        return self.ensure(_connect_error, _declare_consumer)

    def iterconsume(self, limit=None, timeout=None):
        """Return an iterator that will consume from all queues/consumers."""

        info = {'do_consume': True}

        if not timeout:
            timeout = int(int(self.conf.get('heartbeat', 60)) / 2)

        def _error_callback(exc):
            LOG.warning("iterconsume: %s" % str(exc))
            if isinstance(exc, socket.timeout):
                raise RPCException(message='timed out')
            else:
                info['do_consume'] = True

        def _consume():
            if info['do_consume']:
                queues_head = self.consumers[:-1]  # not fanout.
                queues_tail = self.consumers[-1]  # fanout
                for queue in queues_head:
                    queue.consume(nowait=True)
                queues_tail.consume(nowait=False)
                info['do_consume'] = False
            try:
                return self.connection.drain_events(timeout=timeout)
            except:
                self.connection.heartbeat_check()

        for iteration in itertools.count(0):
            if limit and iteration >= limit:
                raise StopIteration
            yield self.ensure(_error_callback, _consume)

    def publisher_send(self, cls, topic, msg, exchange_name, timeout=None, **kwargs):
        """Send to a publisher based on the publisher class."""

        def _error_callback(exc):
            LOG.warning("publisher_send: %s" % str(exc))

        def _publish():
            publisher = cls(self.conf, self.channel, topic, exchange_name, **kwargs)
            publisher.send(msg, timeout)

        self.ensure(_error_callback, _publish)

    def declare_topic_consumer(self, topic, callback=None, queue_name=None,
                               exchange_name=None, ack_on_error=True):
        """Create a 'topic' consumer."""
        self.declare_consumer(TopicConsumer, topic, callback,
                              exchange_name=exchange_name,
                              ack_on_error=ack_on_error,
                              queue_name=queue_name)

    def topic_send(self, topic, msg, exchange_name=None, timeout=None):
        """Send a 'topic' message."""
        if exchange_name is None:
            exchange_name = topic
        self.publisher_send(TopicPublisher, topic, msg, exchange_name, timeout)

    def consume(self, limit=None):
        """Consume from all queues/consumers."""
        it = self.iterconsume(limit=limit)
        while True:
            try:
                it.next()
            except StopIteration:
                return
