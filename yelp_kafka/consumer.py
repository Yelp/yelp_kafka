from collections import defaultdict
from multiprocessing import Event
from multiprocessing import Process
import time
import os
import signal

from kazoo.client import KazooClient
from kafka import KafkaClient
from kafka import SimpleConsumer
from kafka.consumer import AUTO_COMMIT_MSG_COUNT
from kafka.consumer import AUTO_COMMIT_INTERVAL
from kafka.consumer import KafkaUnavailableError

# This is fixed to 1 MB for making a fetch call more efficient when dealing
# with ranger messages can be more than 100KB in size
KAFKA_BUFFER_SIZE = 1024 * 1024
SMALLEST = 0
LARGEST = 1
ZOOKEEPER_BASE_PATH = '/python-kakfa/'
MAX_WAITING_TIME = 10


class Consumer(object):

    def __init__(self, topic, partitions=None):
        assert type(topic) is str and topic
        if type(partitions) is str:
            self.partitions = [partitions]
        else:
            self.partitions = partitions
        self.topic = topic
        self.termination_flag = Event()

    def initialize():
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

    def decompress(self, message):
        """ Decompress the message """
        # TODO: We don't use compression currently
        return message

    def _process(self, kafka_message):
        value = self.decompress(kafka_message.value)
        self.process(kafka_message.key, value)

    def terminate(self):
        """ Terminate the consumer """
        self.termination_flag.set()

    def run(self, brokers, client_id, group,
            offset_set=LARGEST,
            auto_commit_every_n=AUTO_COMMIT_MSG_COUNT,
            auto_commit_every_t=AUTO_COMMIT_INTERVAL):
        """ Fetch and process messages from kafka """

        self.initialize()

        # Instantiate a kafka client connected to kafka.
        client = KafkaClient(brokers, client_id=client_id)

        # Create a kafka SimpleConsumer.
        self.kafka_consumer = SimpleConsumer(
            client, group, self.topic, partitions=self.partitions,
            max_buffer_size=None, buffer_size=KAFKA_BUFFER_SIZE,
            auto_commit_every_n=auto_commit_every_n,
            auto_commit_every_t=auto_commit_every_t
        )

        self._validate_offset()

        for offset, message in self.kafka_consumer:
            self._process(message)
            if self.termination_flag.is_set():
                self._terminate()
                break

    def _terminate(self):
        self.log.info("Terminating consumer topic %s " % self.topic)
        self.kafka_consumer.commit()
        self.client.close()
        self.dispose()

    def _validate_offsets(self):

        """ python-kafka api does not check for offsets validity.
        When either a group does not exist yet or the saved offset is older than the tail
        of the queue the fetch request fails.
        We check the offsets and reset them if necessary."""

        # Disable autocommit to avoid committing offsets during seek
        self.kafka_consumer.auto_commit = False

        group_offsets = self.kafka_consumer.fetch_offsets
        # Fetch the earliest available offset (the older message)
        self.kafka_consumer.seek(0, 0)
        available = self.kafka_consumer.fetch_offsets

        # Validate the group offsets checking that they are > than the earliest
        # available offset
        if any([offset < available[k] for k, offset in group_offsets.iteritems()]):
            self.log.info("Group offset for %s is too old fetching a new valid one"
                          % self.topic)
            if self.offset_reset == LARGEST:
                # Fetch the latest available offset (the newest message)
                self.kafka_consumer.seek(-1, 2)
        else:
            self.kafka_consumer.offsets = group_offsets.copy()
            self.kafka_consumer.fetch_offsets = group_offsets.copy()
        self.kafka_consumer.auto_commit = True


class ConsumerGroup(object):

    def __init__(self, zookeeper_hosts, brokers,
                 client_id, topics, group, create_func):
        self.create_consumer = create_func
        self.brokers = brokers
        self.client_id = client_id
        self.topics = topics
        self.group = group
        self.kazooclient = KazooClient(zookeeper_hosts)
        self.termination_flag = Event()
        self.acquired_partitions = defaultdict(list)

    def start_group(self):

        self.kazooclient.start()
        self.kafkaclient = KafkaClient(self.brokers, client_id=self.client_id)

        # TODO: We load the partitions only once. We should do this
        # periodically and restart the partitioner if partitions changed
        all_partitions = self.get_all_partitions(self.topics)
        group_path = ZOOKEEPER_BASE_PATH + "%s" % self.group

        partitioner = self.kazooclient.SetPartitioner(
            group_path, all_partitions, time_boundary=20
        )

        self.handle_partitions(partitioner)

    def release(self):
        # terminate all the consumer processes
        self._terminate_group()
        self.acquired_partitions.clear()

    def spawn_consumers(self):
        # TODO: We create a consumer process for each partition.
        # There can be too many consumers in case we have a lot of partitions and
        # just a consumer group instance. We should make this smarter and
        # allow the user to decide how many processes/partitions allocate.
        for topic, partitions in self.aquired_partitions.iteritems():
            for p in partitions:
                consumer = self.create_consumer(topic, p)
                self.consumer_procs[self._start_consumer(consumer)] = consumer

    def _start_consumer(self, consumer):
        # Create a new consumer process
        proc = Process(target=consumer.run,
                       args=(
                           self.brokers,
                           self.client_id,
                           self.group,
                       ))
        proc.start()
        return proc

    def handle_partitions(self, partitioner):
        while True:
            if self.termination_flag.wait(1):
                self._terminate_group()
                break
            if partitioner.failed:
                self.log.error("Lost or unable to acquire partitions")
            elif partitioner.release:
                self.release()
                partitioner.release_set()
            elif partitioner.acquired:
                if not self.acquired_partitions:
                    for partition in partitioner:
                        topic, partition_id = partition.split('-')
                        self.acquired_partitions[topic].append(partition_id)
                    self.spawn_consumers()
            elif partitioner.allocating:
                self.log.info("Allocating partitions")
                partitioner.wait_for_acquire()
            self.monitor()

    def get_all_partitions(self):
        """ Load partitions metadata from kafka and create
        a set containing <topic>-<partition_id>
        """
        try:
            self.kafkaclient.load_metadata_for_topics()
        except KafkaUnavailableError:
            # Sometimes the kakfa server closes the connection for inactivity
            # in this case the second call should succeed otherwise the kafka
            # server is down and we should exit
            self.warning("First call to kafka for loading metadata failed."
                         " Trying again.")
            self.kafkaclient.load_metadata_for_topics()

        return set(["{0}-{1}".format(topic, p)
                    for topic, partitions in self.kafkaclient.topic_partitions.iteritems()
                    for p in partitions])

    def terminate_group(self):
        self.termination_flag.set()

    def _terminate_group(self):
        for consumer in self.consumer_procs.itervalues():
            consumer.terminate()

        timeout = time.time() + MAX_WAITING_TIME
        while (time.time() <= timeout and
               any([proc.is_alive() for proc in self.consumer_procs.iterkeys()])):
            continue

        for proc in self.consumer_procs.iterkeys():
            if proc.is_alive():
                os.kill(proc.pid, signal.SIGKILL)
                self.log.error(
                    "Process {0} killed due to timeout".format(proc.pid)
                )
        self.consumer_procs.clear()

    def respawn_consumer(self, proc):
        consumer = self.consumer_procs[proc]
        del self.consumer_procs[proc]
        self.consumer_procs[self._start_consumer(consumer)] = consumer

    def monitor(self):
        for proc in self.consumer_procs.keys():
            if not proc.is_alive():
                self.log.error(
                    "consumer %s died exit status %s" %
                    (proc.name, proc.exitcode))
                self.respawn_consumer(proc)
