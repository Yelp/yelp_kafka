from collections import namedtuple
import logging
from multiprocessing import Event

from kafka import KafkaClient
from kafka import SimpleConsumer

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

    :param topic: topic to consume from.
    :type topic: string.
    :param config: consumer configuration.
    :type config: dict.
    :param partitions: topic partitions to consumer from.
    :type partitions: list.
    """

    def __init__(self, topic, config, partitions=None):
        self.log = logging.getLogger(self.__class__.__name__)
        if not isinstance(topic, str):
            raise TypeError("Topic must be a string")
        self.topic = topic
        if partitions and not isinstance(partitions, list):
            raise TypeError("Partitions must be a list")
        self.partitions = partitions
        self.kafka_consumer = None
        self.config = config

    def connect(self):
        """ Connect to kafka and validate the offsets for a topic.
        It uses config parameters to create a kafka-python
        KafkaClient and SimpleConsumer.
        """
        # Instantiate a kafka client connected to kafka.
        self.client = KafkaClient(self.config.cluster.broker_list,
                                  client_id=self.config.client_id)

        # Create a kafka SimpleConsumer.
        self.kafka_consumer = SimpleConsumer(
            client=self.client, topic=self.topic, partitions=self.partitions,
            **self.config.get_simple_consumer_args()
        )
        self.log.debug(
            "Connected to kafka. Topic %s, partitions %s, %s", self.topic,
            self.partitions, ','.join(
                ['{0} {1}'.format(k, v)
                 for k, v in self.config.get_simple_consumer_args().iteritems()]
            ))
        self.kafka_consumer.provide_partition_info()
        self._validate_offsets(self.config.auto_offset_reset)

    def __iter__(self):
        for partition, kafka_message in self.kafka_consumer:
            # We need to filter out possible old messages.
            # See https://github.com/mumrah/kafka-python/issues/322
            # kafka-python increments the offsets value just before returning the
            # message, so we need to compare the message offset with the
            # current - 1
            if kafka_message[0] >= self.kafka_consumer.offsets[partition] - 1:
                yield Message(
                    partition=partition,
                    offset=kafka_message[0],
                    key=kafka_message[1].key,
                    value=kafka_message[1].value
                )

    def close(self):
        """Disconnect from kafka.
        If auto_commit is enabled commit offsets before disconnecting.
        """
        if self.kafka_consumer.auto_commit is True:
            try:
                self.kafka_consumer.commit()
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
                        time (in seconds) ultil count messages is fetched.
                        If None, it will block forever.

        :returns: a Kafka message
        :rtype: Message namedtuple, which consists of: partition number,
                offset, key, and message value
        """
        while True:
            fetched_message = self.kafka_consumer.get_message(block, timeout)
            if fetched_message is None:
                # get message timed out returns None
                return None
            else:
                partition, kafka_message = fetched_message
                # We need to filter out possible old messages.
                # See https://github.com/mumrah/kafka-python/issues/322
                # kafka-python increments the offsets value just before returning the
                # message, so we need to compare the message offset with the
                # current - 1
                if kafka_message[0] >= self.kafka_consumer.offsets[partition] - 1:
                    return Message(partition=partition, offset=kafka_message[0],
                                   key=kafka_message[1].key, value=kafka_message[1].value)

    def _validate_offsets(self, auto_offset_reset):
        """ Validate the offsets for a topics by comparing the earliest
        available offsets with the consumer group offsets.
        python-kafka api does not check for offsets validity.
        When either a group does not exist yet or the saved offsets
        are older than the tail of the queue the fetch request fails.

        :param latest_offset: If True, the latest_offsets (tail of the queue)
                              are used as new valid offsets. Otherwise, the earliest
                              offsets (head of the queue) are used.
        :type latest_offset: boolean.
        """

        # Disable autocommit to avoid committing offsets during seek
        saved_auto_commit = self.kafka_consumer.auto_commit
        self.kafka_consumer.auto_commit = False

        group_offsets = self.kafka_consumer.fetch_offsets
        # Fetch the earliest available offset (the older message)
        self.kafka_consumer.seek(0, 0)
        available = self.kafka_consumer.fetch_offsets

        # Validate the group offsets checking that they are > than the earliest
        # available offset
        if any([offset < available[k]
                for k, offset in group_offsets.iteritems()]):
            self.log.warning("Group offset for %s is too old..."
                             "Resetting offset", self.topic)
            if auto_offset_reset == 'largest':
                # Fetch the latest available offset (the newest message)
                self.log.debug("Reset to latest offsets")
                self.kafka_consumer.seek(-1, 2)
            else:
                # We don't need to seek the offset again to the earliest
                # offset. Because the first seek call already changed the
                # offsets.
                self.log.debug("Reset to earliest offset")
        else:
            # self.fetch_offsets is used for the kafka fetch request,
            # while self.offsets is used to store the last processed message
            # offsets. When we bootstrap the kafka consumer we need these two
            # dicts to be in sync with one other.
            self.kafka_consumer.offsets = group_offsets.copy()
            self.kafka_consumer.fetch_offsets = group_offsets.copy()
        self.kafka_consumer.auto_commit = saved_auto_commit


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

    def run(self):
        """Fetch and process messages from kafka.
        Non returning function. It initialize the consumer, connect to kafka
        and start processing messages.

        :raises: MessageProcessError when the process function fails
        """
        self.initialize()
        try:
            # We explicitly catch and log the exception.
            self.connect()
        except:
            self.log.exception("Consumer topic %s, partition %s, config %s:"
                               " failed connecting to kafka", self.topic,
                               self.partitions, self.config)
            raise
        while True:
            for message in self:
                try:
                    self.process(message)
                except:
                    self.log.exception("Error processing message: %s", message)
                    raise ProcessMessageError("Error processing message: %s", message)
            if self.termination_flag.is_set():
                self._terminate()
                break

    def _terminate(self):
        """Commit offsets and terminate the consumer.
        """
        self.log.info("Terminating consumer topic %s ", self.topic)
        self.kafka_consumer.commit()
        self.client.close()
        self.dispose()
