from collections import defaultdict
import logging
from multiprocessing import Event
from multiprocessing import Lock
from multiprocessing import Process
import time
import os
import signal

from kazoo.client import KazooClient
from kafka import KafkaClient
from kafka.common import KafkaUnavailableError


ZOOKEEPER_BASE_PATH = '/python-kakfa/'
TIME_BOUNDARY = 30
MAX_WAITING_TIME = 10


class ConsumerGroup(object):

    def __init__(self, zookeeper_hosts, brokers,
                 client_id, topics, group_id):
        self.brokers = brokers
        self.client_id = client_id
        self.topics = topics
        self.group_id = group_id
        self.kazooclient = KazooClient(zookeeper_hosts)
        self.termination_flag = Event()
        self.consumers_lock = Lock()
        self._acquired_partitions = None
        self.consumer_procs = None
        self.log = logging.getLogger(__name__)

    def start_group(self):

        self.kazooclient.start()
        self.kafkaclient = KafkaClient(self.brokers, client_id=self.client_id)

        # TODO: We load the partitions only once. We should do this
        # periodically and restart the partitioner if partitions changed
        all_partitions = self.get_all_partitions(self.topics)
        group_path = ZOOKEEPER_BASE_PATH + self.group_id

        partitioner = self.kazooclient.SetPartitioner(
            group_path, all_partitions, time_boundary=TIME_BOUNDARY
        )

        self.handle_partitions(partitioner)

    def stop_group(self):
        """ Set the termination flag to stop the group """
        self.termination_flag.set()

    def handle_partitions(self, partitioner):
        while True:
            if self.termination_flag.wait(1):
                self._release(partitioner)
                break
            if partitioner.failed:
                self._fail(partitioner)
            elif partitioner.release:
                self._release(partitioner)
            elif partitioner.acquired:
                if not self._acquired_partitions:
                    self._acquire(partitioner)
            elif partitioner.allocating:
                self.log.info("Allocating partitions")
                partitioner.wait_for_acquire()
            self.monitor()

    def _release(self, partitioner):
        self.log.info("Releasing partitions")
        self.release(self._acquired_partitions.copy())
        self._acquired_partitions.clear()
        partitioner.release_set()

    def _fail(self, partitioner):
        self.log.error("Lost or unable to acquire partitions")
        self._acquired_partitions.clear()

    def _acquire(self, partitioner):
        self.log.info("Partitions acquired")
        self._acquired_partitions = self._get_acquired_partitions(partitioner)
        with self.consumers_lock:
            self.allocated_consumers = self.start(
                self._acquired_partitions.copy()
            )

    def get_consumers(self):
        with self.consumers_lock:
            return self.allocated_consumers[:]

    def __iter__(self):
        with self.consumers_lock:
            for consumer in self.allocated_consumers:
                yield consumer

    def _get_acquired_partitions(self, partitioner):
        acquired_partitions = defaultdict(list)
        for partition in partitioner:
            topic, partition_id = partition.split('-')
            acquired_partitions[topic].append(partition_id)
        return acquired_partitions

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
            self.log.warning("First call to kafka for loading metadata failed."
                             " Trying again.")
            self.kafkaclient.load_metadata_for_topics()

        return set(["{0}-{1}".format(topic, p)
                    for topic, partitions in self.kafkaclient.topic_partitions.iteritems()
                    for p in partitions])

    def start(self, acquired_partitions):
        """ Implement the logic to start all the consumers.
        the acquired partitions can be accessed through
        """
        pass

    def release(self, acquired_partitions):
        """ Should implement the logic to terminate
        the consumers.
        """
        pass

    def monitor(self):
        """ Periodically called to monitor the status of consumers.
        Usually checking on the status of the processes should be fine.
        """
        pass


class MultiprocessingConsumerGroup(ConsumerGroup):

    def __init__(self, zookeeper_hosts, brokers,
                 client_id, topics, group_id, consumer_factory):
        super(MultiprocessingConsumerGroup, self).__init__(
            zookeeper_hosts, brokers, client_id, topics, group_id
        )
        self.consumer_factory = consumer_factory

    def start(self, acquired_partitions):
        # TODO: We create a consumer process for each partition.
        # There can be too many consumers in case we have a lot of
        # partitions and just a consumer group instance.
        # We should make this smarter and allow the user to decide
        # how many processes/partitions allocate.
        for topic, partitions in acquired_partitions.iteritems():
            for p in partitions:
                consumer = self.consumer_factory(topic, [p])
                self.consumer_procs[self._start_consumer(consumer)] = consumer

    def _start_consumer(self, consumer):
        # Create a new consumer process
        proc = Process(target=consumer.run,
                       args=(
                           self.brokers,
                           self.client_id,
                           self.group_id,
                       ))
        proc.start()
        return proc

    def release(self):
        # terminate all the consumer processes
        self.log.info("Terminating consumer group")
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
        self.log.info("Respawning consumer %s", proc.name)
        consumer = self.consumer_procs[proc]
        del self.consumer_procs[proc]
        self.consumer_procs[self._start_consumer(consumer)] = consumer

    def monitor(self):
        for proc in self.consumer_procs.keys():
            if not proc.is_alive():
                self.log.error("consumer %s died exit status %s",
                               proc.name, proc.exitcode)
                self.respawn_consumer(proc)
