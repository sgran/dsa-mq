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

from dsa_mq.publisher import FanoutPublisher
from dsa_mq.consumer import FanoutConsumer

LOG = logging.getLogger(__name__)

class RPCException(Exception):

    def __init__(self, message=None, **kwargs):
        if not message:
            message = "An unknown RPC related exception occurred."
        super(RPCException, self).__init__(message)


class Connection(object):
    """Connection object."""

    pool = None

    def __init__(self, conf, logger=None):
        self.conf = conf
        self.consumers         = []
        self.max_retries       = conf.get('max_retries', None)
        self.interval_start    = conf.get('interval_start', None) or 1
        self.interval_stepping = conf.get('interval_stepping', None) or 3
        self.interval_max      = conf.get('interval_max', None) or 30
        self.logger            = logger

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
            }

            params['ssl'] = self._fetch_ssl_params()
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
            if self.conf['ssl_keyfile']:
                ssl_params['keyfile'] = self.conf['ssl_keyfile']
            if self.conf['ssl_certfile']:
                ssl_params['certfile'] = self.conf['ssl_certfile']
            if self.conf['ssl_ca_certs']:
                ssl_params['ca_certs'] = self.conf['ssl_ca_certs']

        if not ssl_params:
            # Just have the default behavior
            return True
        else:
            # Return the extended behavior
            return ssl_params

    def _connect(self, params):
        """Connect to rabbit.  Re-establish any queues that may have
        been declared before if we are reconnecting.  Exceptions should
        be handled by the caller.
        """
        if self.connection:
            try:
                self.connection.release()
            except self.connection_errors:
                pass
            # Setting this in case the next statement fails, though
            # it shouldn't be doing any network operations, yet.
            self.connection = None
        self.connection = kombu.connection.BrokerConnection(**params)
        self.connection_errors = self.connection.connection_errors
        LOG.debug("_connect: %s" % self.connection.connection_errors)
        self.connection.connect()
        self.channel = self.connection.channel()

    def reconnect(self):
        """Handles reconnecting and re-establishing queues.
        Will retry up to self.max_retries number of times.
        self.max_retries = 0 means to retry forever.
        Sleep between tries, starting at self.interval_start
        seconds, backing off self.interval_stepping number of seconds
        each attempt.
        """

        attempt = 0
        while True:
            params = self.params_list[attempt % len(self.params_list)]
            attempt += 1
            try:
                self._connect(params)
                return
            except (IOError, self.connection_errors) as e:
                LOG.info("reconnect: IOerror: %s" % e)
            except Exception as e:
                # NOTE(comstud): Unfortunately it's possible for amqplib
                # to return an error not covered by its transport
                # connection_errors in the case of a timeout waiting for
                # a protocol response.  (See paste link in LP888621)
                # So, we check all exceptions for 'timeout' in them
                # and try to reconnect in this case.
                if 'timeout' not in str(e):
                    LOG.warning(e)

            if self.max_retries and attempt == self.max_retries:
                msg = ('Unable to connect to AMQP server on '
                        '%s:%d after %d '
                        'tries: %s') % (params['hostname'], 
                                        params['port'],
                                        attempt,
                                        e)
                print msg
                raise RPCException(message=msg)

            if attempt == 1:
                sleep_time = self.interval_start or 1
            elif attempt > 1:
                sleep_time += self.interval_stepping
            if self.interval_max:
                sleep_time = min(sleep_time, self.interval_max)

            time.sleep(sleep_time)

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
        self.connection.release()
        self.connection = None

    def reset(self):
        """Reset a connection so it can be used again."""
        self.channel.close()
        self.channel = self.connection.channel()
        self.consumers = []

    def declare_consumer(self, consumer_cls, queue, callback):
        """Create a Consumer using the class that was passed in and
        add it to our list of consumers
        """

        def _connect_error(exc):
            LOG.warning("declare_consumer: %s" % exc)

        def _declare_consumer():
            consumer = consumer_cls(self.conf, self.channel, queue, callback, {})
            self.consumers.append(consumer)
            return consumer
        return self.ensure(_connect_error, _declare_consumer)

    def iterconsume(self, limit=None, timeout=None):
        """Return an iterator that will consume from all queues/consumers."""

        info = {'do_consume': True}

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
            return self.connection.drain_events(timeout=timeout)

        for iteration in itertools.count(0):
            if limit and iteration >= limit:
                raise StopIteration
            yield self.ensure(_error_callback, _consume)

    def publisher_send(self, cls, topic, msg, timeout=None, **kwargs):
        """Send to a publisher based on the publisher class."""

        def _error_callback(exc):
            LOG.warning("publisher_send: %s" % str(exc))

        def _publish():
            publisher = cls(self.conf, self.channel, topic, **kwargs)
            publisher.send(msg, timeout)

        self.ensure(_error_callback, _publish)

    def declare_fanout_consumer(self, queue, callback):
        """Create a 'fanout' consumer."""
        self.declare_consumer(FanoutConsumer, queue, callback)

    def fanout_send(self, topic, msg):
        """Send a 'fanout' message."""
        self.publisher_send(FanoutPublisher, topic, msg)

    def consume(self, limit=None):
        """Consume from all queues/consumers."""
        it = self.iterconsume(limit=limit)
        while True:
            try:
                it.next()
            except StopIteration:
                return
