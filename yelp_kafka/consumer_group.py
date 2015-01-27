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

from yelp_kafka.config import load_config_or_default
from yelp_kafka.error import ConsumerGroupError


class ConsumerGroup(object):
    """Base class to implement consumer group.
    It makes use of kazoo partitioner to achieve partitions
    distribution for a list of topics between many consumer instances.

    :param zookeeper_hosts: zookeeper hosts
    :type zookeeper_hosts: list ["host:port"]
    :param topics: topics to consume from
    :type topics: list
    :param config: yelp_kakfa config. See :py:mod:`yelp_kafka.config`
    :type config: dict

    .. note: This class is thread safe.
    """

    def __init__(self, zookeeper_hosts, topics, config):
        self.log = logging.getLogger(__name__)
        if not isinstance(zookeeper_hosts, list):
            raise TypeError("zookeeper_hosts must be a list")
        self.config = load_config_or_default(config)
        self.log.debug("Using config: %s", self.config)
        self.topics = topics
        self.kazooclient = KazooClient(','.join(zookeeper_hosts))
        self.termination_flag = None
        self.consumers_lock = Lock()
        self._acquired_partitions = defaultdict(list)
        self.partitioner = None
        self.allocated_consumers = None

    def get_group_path(self):
        """Get the group path in zookeeper."""
        return '/'.join([self.config['zookeeper_base'],
                        self.config['group_id']])

    def start_group(self):
        """Start and continuously monitor the consumer group."""
        # TODO (KAFKA-71): We load the partitions only once. We should do this
        # periodically and restart the partitioner if partitions changed.
        # Actually kazoo partitioner seems to have some issues if we change the
        # partition set on the fly. From my tests I saw partitions not being
        # allocated after recreating the partitioner. Need to investigate more.

        self.kazooclient.start()
        # Create the termination flag
        self.termination_flag = Event()

        while not self.termination_flag.is_set():
            if not self.partitioner:
                # FIXME: When the partitioner fails we should restart a kazooclient.
                self.partitioner = self.kazooclient.SetPartitioner(
                    path=self.get_group_path(),
                    set=self.get_all_partitions(),
                    time_boundary=self.config['zk_partitioner_cooldown']
                )
            self._handle_partitions()
            self.termination_flag.wait(1)
        # Release the group for termination
        self._release()

    def stop_group(self):
        """ Set the termination flag to stop the group """
        if not self.termination_flag:
            raise ConsumerGroupError("Group not running")
        self.termination_flag.set()

    def _handle_partitions(self):
        """Handle group status changes, for example when a new
        consumer joins or leaves the group.
        """
        if self.partitioner.failed:
            self._fail()
        elif self.partitioner.release:
            self._release()
        elif self.partitioner.acquired:
            if not self._acquired_partitions:
                self.log.info("Allocation done!")
                self._acquire()
            self.monitor()
        elif self.partitioner.allocating:
            self.log.info("Allocating partitions")
            self.partitioner.wait_for_acquire()

    def _release_consumers(self):
        """Release the allocated consumers calling the release function."""
        self.release(self._acquired_partitions)
        self._acquired_partitions.clear()
        with self.consumers_lock:
            self.allocated_consumers = None

    def _release(self):
        """Release the consumers and acquired partitions.
        This function is executed either at termination time or
        whenever there is a group change.
        """
        self.log.info("Releasing partitions")
        self._release_consumers()
        self.partitioner.release_set()

    def _fail(self):
        """Release the consumers.
        This function is executed when there is a failure in zookeeper and
        the consumer group is not able to recover the connection. In this case
        we can't release the acquired partitions (no connection with zookeeper),
        but we cowardly stop the running consumers.
        """
        self.log.error("Lost or unable to acquire partitions")
        if self._acquired_partitions:
            self._release_consumers()
        self.partitioner = None

    def _acquire(self):
        """Acquire kafka topics-[partitions] and start the
        consumers for them.
        """
        self.log.info("Partitions acquired")
        self._acquired_partitions = self._get_acquired_partitions(self.partitioner)
        self.log.info("Acquired partitions: %s", self._acquired_partitions)
        with self.consumers_lock:
            self.allocated_consumers = self.start(
                self._acquired_partitions
            )
            self.log.info("Allocated consumers %s", self.allocated_consumers)

    def get_consumers(self):
        """Get a copy of the allocated consumers.

        :returns: A copy of allocated consumers
        :rtype: list
        """
        with self.consumers_lock:
            if self.allocated_consumers is not None:
                return self.allocated_consumers[:]
            return None

    def _get_acquired_partitions(self, partitioner):
        """Retrieve acquired partitions from a partitioner.

        :returns: acquired topic and partitions
        :rtype: dict {<topic>: <[partitions]>}

        """
        acquired_partitions = defaultdict(list)
        for partition in partitioner:
            topic, partition_id = partition.split('-')
            acquired_partitions[topic].append(int(partition_id))
        return acquired_partitions

    def get_all_partitions(self):
        """ Load partitions metadata from kafka and create
        a set containing "<topic>-<partition_id>"

        :returns: partitions for user topics
        :rtype: set
        """

        kafkaclient = KafkaClient(
            self.config['brokers'],
            client_id=self.config['client_id']
        )
        try:
            kafkaclient.load_metadata_for_topics()
        except KafkaUnavailableError:
            # Sometimes the kakfa server closes the connection for inactivity
            # in this case the second call should succeed otherwise the kafka
            # server is down and we should fail
            self.log.warning("First call to kafka for loading metadata failed."
                             " Trying again.")
            kafkaclient.load_metadata_for_topics()

        partitions = []
        for topic in self.topics:
            if topic not in kafkaclient.topic_partitions:
                self.log.warning("Topic %s does not exist in kafka", topic)
            else:
                partitions += ["{0}-{1}".format(topic, p)
                               for p in kafkaclient.topic_partitions[topic]]
        return set(partitions)

    def start(self, acquired_partitions):
        """ Start all the consumers.

        :param acquired_partitions: acquired topics and partitions
        :type acquired_partitions: dict {<topic>: <[partitions]>}
        :returns: a list of the allocated consumers
        :rtype: list

        .. note: implement in subclass.
        .. note: Must return a list of the allocated consumers (subclass of Consumer)
        """
        pass

    def release(self, acquired_partitions):
        """ Release the consumers.

        :param acquired_partitions: acquired topics and partitions
        :type acquired_partitions: dict {<topic>: <[partitions]>}

        .. note: implement in subclass.
        """
        pass

    def monitor(self):
        """ Periodically called to monitor the status of consumers.
        Usually, checking if consumers are alive should be fine.
        """
        pass


class MultiprocessingConsumerGroup(ConsumerGroup):
    """ MultiprocessingConsumerGroup class. Inherit from
    :class:`yelp_kafka.consumer_group.ConsumerGroup`.

    It makes use of multiprocessing to instantiate a consumer process per topic
    partition. It also supports process monitoring and re-spawning upon
    consumer failure.

    :param consumer_factory: the function used to instantiate the consumer object
    :type consumer_factory: func
    """

    def __init__(self, zookeeper_hosts, topics,
                 config, consumer_factory):
        super(MultiprocessingConsumerGroup, self).__init__(
            zookeeper_hosts, topics, config
        )
        self.consumer_factory = consumer_factory
        self.consumer_procs = {}

    def start(self, acquired_partitions):
        """Start a consumer process for each acquired partition.
        Use consumer_factory to create a consumer instance. Then
        start a new process on the method run.

        :param acquired_partitions: acquired topics partitions
        :type: dict {<topic>: <[partitions]>}
        """
        # TODO: We create a consumer process for each partition.
        # There can be too many consumers in case we have a lot of
        # partitions and just a worker instance.
        # We should make this smarter and allow the user to decide
        # how many processes/partitions allocate.
        for topic, partitions in acquired_partitions.iteritems():
            for p in partitions:
                self.log.info(
                    "Creating consumer topic = %s, config = %s,"
                    " partition = %s", topic, self.config, p
                )
                consumer = self.consumer_factory(topic, self.config.copy(), [p])
                self.consumer_procs[self._start_consumer(consumer)] = consumer
        return self.consumer_procs.values()

    def _start_consumer(self, consumer):
        """Create a new consumer process"""
        proc = Process(target=consumer.run, name='Consumer-{0}-{1}'.format(consumer.topic, consumer.partitions))
        proc.start()
        return proc

    def release(self, acquired_partitions):
        """Terminate all the consumer processes"""
        self.log.info("Terminating consumer group")
        for consumer in self.consumer_procs.itervalues():
            consumer.terminate()

        timeout = time.time() + self.config['max_termination_timeout_secs']
        while (time.time() <= timeout and
               any([proc.is_alive() for proc in self.consumer_procs.iterkeys()])):
            continue

        for proc, consumer in self.consumer_procs.iteritems():
            if proc.is_alive():
                os.kill(proc.pid, signal.SIGKILL)
                self.log.error(
                    "Process %s, topic %s, partitions %s: killed due to timeout", proc.name,
                    consumer.topic, consumer.partitions
                )
        self.consumer_procs.clear()

    def monitor(self):
        """Respawn consumer processes upon failures."""
        for proc, consumer in self.consumer_procs.items():
            if not proc.is_alive():
                self.log.error("consumer process %s topic %s partitions %s: "
                               "died exit status %s", proc.name, consumer.topic,
                               consumer.partitions, proc.exitcode)
                # Restart consumer process
                self.consumer_procs[self._start_consumer(consumer)] = consumer
                # Remove dead process
                del self.consumer_procs[proc]
