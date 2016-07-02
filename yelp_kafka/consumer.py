# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from collections import namedtuple
from multiprocessing import Event

import six
from kafka import KafkaClient
from kafka import SimpleConsumer
from kafka.common import KafkaError
from kafka.common import OffsetCommitRequest
from kafka.util import kafka_bytestring
from setproctitle import getproctitle
from setproctitle import setproctitle

from yelp_kafka.error import ProcessMessageError


Message = namedtuple("Message", ["partition", "offset", "key", "value"])
"""Tuple representing a kafka message.

* **partition**\(``int``): The partition number of the message
* **offset**\(``int``): Message offset
* **key**\(``str``): Message key
* **value**\(``str``): Message value
"""


class KafkaSimpleConsumer(object):
    """ Base class for consuming from kafka.
    Implement the logic to connect to kafka and consume messages.
    KafkaSimpleConsumer is a wrapper around kafka-python SimpleConsumer.
    KafkaSimpleConsumer relies on it in order to consume messages from kafka.
    KafkaSimpleConsumer does not catch exceptions raised by kafka-python.

    An instance of this class can be used as iterator
    to consume messages from kafka.

    .. warning:: This class is considered deprecated in favor of
                 K:py:class:`yelp_kafka.consumer_group.KafkaConsumerGroup`.

    :param topic: topic to consume from.
    :type topic: string.
    :param config: consumer configuration.
    :type config: dict.
    :param partitions: topic partitions to consumer from.
    :type partitions: list.
    """

    def __init__(self, topic, config, partitions=None):
        self.log = logging.getLogger(self.__class__.__name__)
        if not isinstance(topic, six.string_types):
            raise TypeError("Topic must be a string")
        self.topic = kafka_bytestring(topic)
        if partitions and not isinstance(partitions, list):
            raise TypeError("Partitions must be a list")
        self.partitions = partitions
        self.kafka_consumer = None
        self.config = config

    def connect(self):
        """ Connect to kafka and create a consumer.
        It uses config parameters to create a kafka-python
        KafkaClient and SimpleConsumer.
        """
        # Instantiate a kafka client connected to kafka.
        self.client = KafkaClient(
            self.config.broker_list,
            client_id=self.config.client_id
        )

        # Create a kafka SimpleConsumer.
        self.kafka_consumer = SimpleConsumer(
            client=self.client, topic=self.topic, partitions=self.partitions,
            **self.config.get_simple_consumer_args()
        )
        self.log.debug(
            "Connected to kafka. Topic %s, partitions %s, %s",
            self.topic,
            self.partitions,
            ','.join(['{0} {1}'.format(k, v) for k, v in
                      six.iteritems(self.config.get_simple_consumer_args())])
        )
        self.kafka_consumer.provide_partition_info()

    def __iter__(self):
        for partition, kafka_message in self.kafka_consumer:
            yield Message(
                partition=partition,
                offset=kafka_message[0],
                key=kafka_message[1].key,
                value=kafka_message[1].value,
            )

    def __enter__(self):
        self.connect()

    def __exit__(self, type, value, tb):
        self.close()

    def close(self):
        """Disconnect from kafka.
        If auto_commit is enabled commit offsets before disconnecting.
        """
        if self.kafka_consumer.auto_commit is True:
            try:
                self.commit()
            except:
                self.log.exception("Commit error. "
                                   "Offsets may not have been committed")
        # Close all the connections to kafka brokers. KafkaClient open
        # connections to all the partition leaders.
        self.client.close()

    def get_message(self, block=True, timeout=0.1):
        """Get message from kafka. It supports the same arguments of get_message
        in kafka-python SimpleConsumer.

        :param block: If True, the API will block till at least a message is fetched.
        :type block: boolean
        :param timeout: If block is True, the function will block for the specified
                        time (in seconds).
                        If None, it will block forever.

        :returns: a Kafka message
        :rtype: Message namedtuple, which consists of: partition number,
                offset, key, and message value
        """
        fetched_message = self.kafka_consumer.get_message(block, timeout)
        if fetched_message is None:
            # get message timed out returns None
            return None
        else:
            partition, kafka_message = fetched_message
            return Message(
                partition=partition,
                offset=kafka_message[0],
                key=kafka_message[1].key,
                value=kafka_message[1].value,
            )

    def commit(self, partitions=None):
        """Commit offset for this consumer group
        :param partitions: list of partitions to commit, default commits to all
        partitions.
        :return: True on success, False on failure.
        """
        if partitions:
            return self.kafka_consumer.commit(partitions)
        else:
            return self.kafka_consumer.commit()

    def commit_message(self, message):
        """Commit the message offset for this consumer group. This function does not
        take care of the consumer offset tracking. It should only be used if
        auto_commit is disabled and the commit function never called.

        .. note:: all the messages received before message itself will be committed
                  as consequence.

        :param message: message to commit.
        :type message: Message namedtuple, which consists of: partition number,
                       offset, key, and message value
        :return: True on success, False on failure.
        """
        reqs = [
            OffsetCommitRequest(
                self.topic,
                message.partition,
                message.offset,
                None,
            )
        ]

        try:
            if self.config.offset_storage in ['zookeeper', 'dual']:
                self.client.send_offset_commit_request(self.config.group_id, reqs)
            if self.config.offset_storage in ['kafka', 'dual']:
                self.client.send_offset_commit_request_kafka(self.config.group_id, reqs)
        except KafkaError as e:
            self.log.error("%s saving offsets: %s", e.__class__.__name__, e)
            return False
        else:
            return True


class KafkaConsumerBase(KafkaSimpleConsumer):
    """Kafka Consumer class. Inherit from
    :class:`yelp_kafka.consumer.KafkaSimpleConsumer`.

    Convenient base class to implement new kafka consumers with
    message processing logic.
    .. note: This class is thread safe.
    """

    def __init__(self, topic, config, partitions=None):
        super(KafkaConsumerBase, self).__init__(topic, config, partitions)
        self.termination_flag = Event()

    def initialize(self):
        """Initialize the consumer.
        When using in multiprocessing, this function should re-configure
        the logger instance (self.log), since it appears to be no longer
        working after the fork.
        Called only once when the consumer starts, and before connecting to kafka.

        .. note: implement in subclass.
        """
        pass

    def dispose(self):
        """Called after offsets commit and kafka connection termination.
        It is executed just before exiting the consumer loop.

        .. note: implement in subclass.
        """
        pass

    def process(self, message):
        """Process a messages.

        .. note: implement in subclass.

        :param message: message to process
        :type message: Message
        """
        pass

    def terminate(self):
        """Terminate the consumer.
        Set a termination variable. The consumer is terminated as soon
        as it receives the next message are the iter_timeout expires.
        """
        self.termination_flag.set()

    def set_process_name(self):
        """Setup process name for consumer to include topic and
        partitions to improve debuggability.
        """
        process_name = '%s-%s-%s' % (getproctitle(), self.topic, self.partitions)
        setproctitle(process_name)

    def run(self):
        """Fetch and process messages from kafka.
        Non returning function. It initialize the consumer, connect to kafka
        and start processing messages.

        :raises: MessageProcessError when the process function fails
        """
        # Setup process name for debuggability
        self.set_process_name()

        self.initialize()
        try:
            # We explicitly catch and log the exception.
            self.connect()
        except:
            self.log.exception(
                "Consumer topic %s, partition %s, config %s:"
                " failed connecting to kafka",
                self.topic,
                self.partitions,
                self.config
            )
            raise
        while True:
            for message in self:
                try:
                    self.process(message)
                except:
                    self.log.exception("Error processing message: %s", message)
                    raise ProcessMessageError(
                        "Error processing message: %s",
                        message,
                    )
            if self.termination_flag.is_set():
                self._terminate()
                break

    def _terminate(self):
        """Commit offsets and terminate the consumer.
        """
        self.log.info("Terminating consumer topic %s ", self.topic)
        self.commit()
        self.client.close()
        self.dispose()
