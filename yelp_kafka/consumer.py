from collections import namedtuple
import logging
from multiprocessing import Event

from kafka import KafkaClient
from kafka import SimpleConsumer

from yelp_kafka.config import load_config_or_default
from yelp_kafka.config import CONSUMER_CONFIG_KEYS

Message = namedtuple("Message", ["partition", "offset", "key", "value"])


class KafkaSimpleConsumer(object):

    def __init__(self, topic, config, partitions=None):
        self.log = logging.getLogger(__name__)
        if not isinstance(topic, str):
            raise TypeError("Topic must be a string")
        self.topic = topic
        if partitions and not isinstance(partitions, list):
            raise TypeError("Partitions must be a list")
        self.partitions = partitions
        self.kafka_consumer = None
        self._config = load_config_or_default(config)

    def connect(self):
        """ Connect to kafka and validate the offsets for a topic """
        # Instantiate a kafka client connected to kafka.
        self.client = KafkaClient(self._config['brokers'],
                                  client_id=self._config['client_id'])

        # Create a kafka SimpleConsumer.
        self.kafka_consumer = SimpleConsumer(
            self.client, self._config['group_id'], self.topic, partitions=self.partitions,
            **dict([(k, self._config[k]) for k in CONSUMER_CONFIG_KEYS])
        )
        self.log.debug("Connected to kafka. Topic %s, group %s, partitions %s, %s",
                       self.topic, self._config['group_id'], self.partitions,
                       ','.join(['%{0} %{1}'.format(k, self._config[k])
                                 for k in CONSUMER_CONFIG_KEYS]))
        self.kafka_consumer.provide_partition_info()
        self._validate_offsets(self._config['latest_offset'])

    def __iter__(self):
        while True:
            for partition, kafka_message in self.kafka_consumer:
                yield Message(
                    partition=partition,
                    offset=kafka_message[0],
                    key=kafka_message[1].key,
                    value=kafka_message[1].value
                )

    def get_message(self, block=True, timeout=0.1):
        """ Get a message from kafka. It has the same arguments of get_message
        in kafka-python SimpleConsumer.

        :returns: a Kafka message
        :rtype: KeyAndMessage
        """
        partition, kafka_message = self.kafka_consumer.get_message(block, timeout)
        return Message(partition=partition, offset=kafka_message[0],
                       key=kafka_message[1].key, value=kafka_message[1].value)

    def _validate_offsets(self, latest_offset):
        """ python-kafka api does not check for offsets validity.
        When either a group does not exist yet or the saved offset
        is older than the tail of the queue the fetch request fails.
        We check the offsets and reset them if necessary.
        """

        # Disable autocommit to avoid committing offsets during seek
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
            if latest_offset is True:
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
        self.kafka_consumer.auto_commit = True


class KafkaConsumer(KafkaSimpleConsumer):

    def __init__(self, topic, config, partitions=None):
        super(KafkaConsumer, self).__init__(topic, config, partitions)
        self.termination_flag = Event()

    def initialize(self):
        """ Called only once when the consumer starts.
        Subclasses can implement this method.
        """
        pass

    def dispose(self):
        """ Called after terminating kafka connection and committing the offset.
        It is executed just before system exit.
        """
        pass

    def process(self, message):
        """ Should implement the application logic.
        :param message: message to process
        :type message: Message
        """
        pass

    def terminate(self):
        """ Terminate the consumer """
        self.termination_flag.set()

    def run(self):
        """ Fetch and process messages from kafka """

        self.initialize()
        self.connect()

        for message in self:
            self.process(message)
            if self.termination_flag.is_set():
                self._terminate()
                break

    def _terminate(self):
        self.log.info("Terminating consumer topic %s ", self.topic)
        self.kafka_consumer.commit()
        self.client.close()
        self.dispose()
