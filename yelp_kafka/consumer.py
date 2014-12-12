from collections import namedtuple
from multiprocessing import Event

from kafka import KafkaClient
from kafka import SimpleConsumer as KafkaSimpleConsumer
from kafka.consumer import AUTO_COMMIT_MSG_COUNT
from kafka.consumer import AUTO_COMMIT_INTERVAL

# This is fixed to 1 MB for making a fetch call more efficient when dealing
# with ranger messages can be more than 100KB in size
KAFKA_BUFFER_SIZE = 1024 * 1024

KeyAndMessage = namedtuple("KeyAndMessage", ["key", "message"])


class SimpleConsumer(object):

    def __init__(self, topic, partitions=None):
        if not isinstance(topic, str):
            raise TypeError("Topic is not a string")
        self.topic = topic

        if not isinstance(partitions, list):
            raise TypeError("Partitions is not a list")
        self.partitions = partitions

    def connect(self, brokers, client_id, group_id,
                latest_offset=True,
                auto_commit_every_n=AUTO_COMMIT_MSG_COUNT,
                auto_commit_every_t=AUTO_COMMIT_INTERVAL):

        # Instantiate a kafka client connected to kafka.
        client = KafkaClient(brokers, client_id=client_id)

        # Create a kafka SimpleConsumer.
        self.kafka_consumer = KafkaSimpleConsumer(
            client, group_id, self.topic, partitions=self.partitions,
            max_buffer_size=None, buffer_size=KAFKA_BUFFER_SIZE,
            auto_commit_every_n=auto_commit_every_n,
            auto_commit_every_t=auto_commit_every_t
        )

        self._validate_offsets(latest_offset)

    def __iter__(self):
        for offset, kafka_message in self.kafka_consumer:
            # TODO: We probably don't want to have the magic and attributes in
            # the message. According with
            yield KeyAndMessage(kafka_message.key, kafka_message.value)

    def get_message(self, block=True, timeout=0.1):
        offset, kafka_message = self.kafka_consumer.get_message(block, timeout)
        return KeyAndMessage(kafka_message.key, kafka_message.value)

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
            self.log.info("Group offset for %s is too old fetching"
                          " a new valid one", self.topic)
            if latest_offset is True:
                # Fetch the latest available offset (the newest message)
                self.kafka_consumer.seek(-1, 2)
        else:
            # self.fetch_offsets is used for the kafka fetch request,
            # while self.offsets is used to store the last processed message
            # offsets. When we bootstrap the kafka consumer we need these two
            # dicts to be in sync with one other.
            self.kafka_consumer.offsets = group_offsets.copy()
            self.kafka_consumer.fetch_offsets = group_offsets.copy()
        self.kafka_consumer.auto_commit = True


class Consumer(object):

    def __init__(self, topic, partitions=None):
        super(Consumer, self).__init__(topic, partitions)
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

    def process(self, key, value):
        """ Should implement the application logic """
        pass

    def terminate(self):
        """ Terminate the consumer """
        self.termination_flag.set()

    def run(self, brokers, client_id, group_id,
            latest_offset=True,
            auto_commit_every_n=AUTO_COMMIT_MSG_COUNT,
            auto_commit_every_t=AUTO_COMMIT_INTERVAL):
        """ Fetch and process messages from kafka """

        self.initialize()

        self.connect(brokers, client_id, group_id,
                     latest_offset, auto_commit_every_n,
                     auto_commit_every_t)

        for key, message in self:
            self.process(key, message)
            if self.termination_flag.is_set():
                self._terminate()
                break

    def _terminate(self):
        self.log.info("Terminating consumer topic %s ", self.topic)
        self.kafka_consumer.commit()
        self.client.close()
        self.dispose()
