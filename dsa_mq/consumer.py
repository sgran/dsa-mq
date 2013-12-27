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

import uuid

import kombu
import kombu.entity

class ConsumerBase(object):
    """Consumer base class."""

    def __init__(self, channel, callback=None, tag=uuid.uuid4().hex, **kwargs):
        """Declare a queue on an amqp channel.

        :param channel: the amqp channel to use
        :param callback: the callback to call when messages are received
        :param tag: is a unique ID for the consumer on the channel

        queue name, exchange name, and other kombu options are
        passed in here as a dictionary.
        """
        self.callback = callback
        self.tag = str(tag)
        self.kwargs = kwargs
        self.queue = None
        self.ack_on_error = kwargs.get('ack_on_error', True)
        self.reconnect(channel)

    def reconnect(self, channel):
        """Re-declare the queue after a rabbit reconnect."""
        self.channel = channel
        self.kwargs['channel'] = channel
        self.queue = kombu.entity.Queue(**self.kwargs)

    def _callback_handler(self, message, callback):
        """Call callback with deserialized message.

        Messages that are processed without exception are ack'ed.

        If the message processing generates an exception, it will be
        ack'ed if ack_on_error=True. Otherwise it will be .reject()'ed.
        Rejection is better than waiting for the message to timeout.
        Rejected messages are immediately requeued.
        """

        ack_msg = False
        try:
            msg = message.payload
            callback(msg)
            ack_msg = True
        except Exception:
            if self.ack_on_error:
                ack_msg = True
        finally:
            if ack_msg:
                message.ack()
            else:
                message.reject()

    def consume(self, *args, **kwargs):
        """Actually declare the consumer on the amqp channel.  This will
        start the flow of messages from the queue.  Using the
        Connection.iterconsume() iterator will process the messages,
        calling the appropriate callback.

        If a callback is specified in kwargs, use that.  Otherwise,
        use the callback passed during __init__()

        If kwargs['nowait'] is True, then this call will block until
        a message is read.

        """

        options = {'consumer_tag': self.tag}
        options['nowait'] = kwargs.get('nowait', False)
        callback = kwargs.get('callback', self.callback)
        if not callback:
            raise ValueError("No callback defined")

        def _callback(raw_message):
            message = self.channel.message_to_python(raw_message)
            self._callback_handler(message, callback)

        self.queue.consume(*args, callback=_callback, **options)

    def cancel(self):
        """Cancel the consuming from the queue, if it has started."""
        try:
            self.queue.cancel(self.tag)
        except KeyError as e:
            # NOTE(comstud): Kludge to get around a amqplib bug
            if str(e) != "u'%s'" % self.tag:
                raise
        self.queue = None


class FanoutConsumer(ConsumerBase):
    """Consumer class for 'fanout'."""

    def __init__(self, conf, channel, queue, callback, tag, **kwargs):
        """Init a 'fanout' queue.

        :param channel: the amqp channel to use
        :param callback: the callback to call when messages are received
        :param tag: a unique ID for the consumer on the channel

        Other kombu options may be passed as keyword arguments
        """
        queue_name = queue

        # Default options
        options = {'durable': True,
                   'queue_arguments': {'x-ha-policy': 'all'},
                   'auto_delete': False,
                   'exclusive': False}
        options.update(kwargs)
        super(FanoutConsumer, self).__init__(channel, callback, tag,
                                             name=queue_name,
                                             **options)


class DirectConsumer(ConsumerBase):
    """Queue/consumer class for 'direct'."""

    def __init__(self, conf, channel, msg_id, callback, tag, **kwargs):
        """Init a 'direct' queue.

        :param channel: the amqp channel to use
        :param msg_id: the msg_id to listen on
        :param callback: the callback to call when messages are received
        :param tag: a unique ID for the consumer on the channel

        Other kombu options may be passed as keyword arguments
        """
        # Default options
        options = {'durable': True,
                   'queue_arguments': {'x-ha-policy': 'all'},
                   'auto_delete': False,
                   'exclusive': False}
        options.update(kwargs)
        exchange = kombu.entity.Exchange(name=msg_id,
                                         type='direct',
                                         durable=options['durable'],
                                         auto_delete=options['auto_delete'])
        super(DirectConsumer, self).__init__(channel,
                                             callback,
                                             tag,
                                             name=msg_id,
                                             exchange=exchange,
                                             routing_key=msg_id,
                                             **options)


class TopicConsumer(ConsumerBase):
    """Consumer class for 'topic'."""

    def __init__(self, conf, channel, topic, callback, tag, name=None,
                 exchange_name=None, **kwargs):
        """Init a 'topic' queue.

        :param channel: the amqp channel to use
        :param topic: the topic to listen on
        :paramtype topic: str
        :param callback: the callback to call when messages are received
        :param tag: a unique ID for the consumer on the channel
        :param name: optional queue name, defaults to topic
        :paramtype name: str

        Other kombu options may be passed as keyword arguments
        """
        # Default options
        options = {'durable': True,
                   'queue_arguments': {'x-ha-policy': 'all'},
                   'auto_delete': False,
                   'exclusive': False}
        options.update(kwargs)
        exchange_name = exchange_name or topic
        exchange = kombu.entity.Exchange(name=exchange_name,
                                         type='topic',
                                         durable=options['durable'],
                                         auto_delete=options['auto_delete'])
        super(TopicConsumer, self).__init__(channel,
                                            callback,
                                            tag,
                                            name=name or topic,
                                            exchange=exchange,
                                            routing_key=topic,
                                            **options)
