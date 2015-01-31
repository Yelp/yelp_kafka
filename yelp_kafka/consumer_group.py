import logging
from multiprocessing import Event
from multiprocessing import Lock
from multiprocessing import Process
import time
import os
import signal

from yelp_kafka.config import load_config_or_default
from yelp_kafka.error import ConsumerGroupError
from yelp_kafka.partitioner import Partitioner
from yelp_kafka.consumer import KafkaSimpleConsumer


DEFAULT_REFRESH_TIMEOUT_IN_SEC = 5


class ConsumerGroup(object):
    """Single process consumer group.
    Support partitions distribution for a single topic
    between many consumer instances.

    :param topics: topics to consume from
    :type topics: list
    :param config: yelp_kakfa config. See :py:mod:`yelp_kafka.config`
    :type config: dict

    .. note: This class is thread safe.
    """

    def __init__(self, topic, config, process_func):
        self.log = logging.getLogger(self.__class__.__name__)
        self._config = load_config_or_default(config)
        self.log.debug("Using config: %s", self._config)
        self.topic = topic
        self.group = Partitioner(
            config,
            topic if isinstance(topic, list) else [topic],
            self._acquire,
            self._release
        )
        self.consumer = None
        self.process = process_func

    def run(self, refresh_timeout=DEFAULT_REFRESH_TIMEOUT_IN_SEC):
        """Create the group, instantiate a consumer and consume message
        from kafka.

        :param refresh_timeout: waiting timeout in secs to refresh
            the consumer group
        """
        self.group.start()
        while True:
            self.consume(refresh_timeout)

    def consume(self, refresh_timeout):
        timeout = time.time() + refresh_timeout
        for message in self.consumer:
            self.process(message)
            if time.time() > timeout:
                break
        self.group.refresh()

    def _acquire(self, partitions):
        """Create a consumer from the acquired partitions.

        .. note: This function should only by called by the partitioner.
        """
        self.consumer = KafkaSimpleConsumer(self.topic, self._config,
                                            partitions[self.topic])
        try:
            # We explicitly catch and log the exception.
            self.consumer.connect()
        except:
            self.log.exception("Consumer topic %s, partition %s, config %s:"
                               " failed connecting to kafka", self.topic,
                               partitions, self._config)
            raise

    def _release(self, partitions):
        """Release the consumer.

        .. note: This function should only by called by the partitioner.
        """
        self.consumer.close()
        self.consumer = None


class MultiprocessingConsumerGroup(object):
    """MultiprocessingConsumerGroup class. Inherit from
    :class:`yelp_kafka.consumer_group.ConsumerGroup`.

    It makes use of multiprocessing to instantiate a consumer process per topic
    partition. It also supports process monitoring and re-spawning upon
    consumer failure.

    :param consumer_factory: the function used to instantiate the consumer object
    :type consumer_factory: func
    """

    def __init__(self, topics, config, consumer_factory):
        self._config = load_config_or_default(config)
        self.termination_flag = None
        self.group = Partitioner(config, topics, self.acquire, self.release)
        self.consumers = None
        self.consumers_lock = Lock()
        self.consumer_procs = {}
        self.consumer_factory = consumer_factory
        self.log = logging.getLogger(self.__class__.__name__)

    def start_group(self, refresh_timeout=DEFAULT_REFRESH_TIMEOUT_IN_SEC):
        """Start and continuously monitor the consumer group."""
        # Create the termination flag
        self.termination_flag = Event()
        self.group.start()
        while not self.termination_flag.is_set():
            self.termination_flag.wait(refresh_timeout)
            self.monitor()
            self.refresh()
        # Release the group for termination
        self.group.stop()

    def stop_group(self):
        """Set the termination flag to stop the group """
        if not self.termination_flag:
            raise ConsumerGroupError("Group not running")
        self.termination_flag.set()

    def acquire(self, partitions):
        """Acquire kafka topics-[partitions] and start the
        consumers for them.
        """
        self.log.debug("Acquired partitions: %s", partitions)
        with self.consumers_lock:
            self.consumers = self.start(
                partitions
            )
            self.log.debug("Allocated consumers %s", self.consumers)

    def start(self, acquired_partitions):
        """Start a consumer process for each acquired partition.
        Use consumer_factory to create a consumer instance. Then
        start a new process on the method run.

        :param acquired_partitions: acquired topics partitions
        :type: dict {<topic>: <[partitions]>}
        """
        # TODO KAFKA-72: We create a consumer process for each partition.
        # There can be too many consumers in case we have a lot of
        # partitions and just a worker instance.
        # We should make this smarter and allow the user to decide
        # how many processes/partitions allocate.
        for topic, partitions in acquired_partitions.iteritems():
            for p in partitions:
                self.log.info(
                    "Creating consumer topic = %s, config = %s,"
                    " partition = %s", topic, self._config, p
                )
                consumer = self.consumer_factory(topic, self._config.copy(), [p])
                self.consumer_procs[self._start_consumer(consumer)] = consumer
        return self.consumer_procs.values()

    def _start_consumer(self, consumer):
        """Create a new consumer process"""
        proc = Process(target=consumer.run, name='Consumer-{0}-{1}'.format(consumer.topic, consumer.partitions))
        proc.start()
        return proc

    def release(self, partitions):
        """Terminate all the consumer processes"""
        self.log.warning("Terminating consumer group")
        for consumer in self.consumer_procs.itervalues():
            consumer.terminate()

        timeout = time.time() + self._config['max_termination_timeout_secs']
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
        with self.consumers_lock:
            self.consumers = None

    def monitor(self):
        """Respawn consumer processes upon failures."""
        # We don't use the iterator because the dict may change during the loop
        for proc, consumer in self.consumer_procs.items():
            if not proc.is_alive():
                self.log.error("consumer process %s topic %s partitions %s: "
                               "died exit status %s", proc.name, consumer.topic,
                               consumer.partitions, proc.exitcode)
                # Restart consumer process
                self.consumer_procs[self._start_consumer(consumer)] = consumer
                # Remove dead process
                del self.consumer_procs[proc]

    def get_consumers(self):
        """Get a copy of the allocated consumers.

        :returns: A copy of allocated consumers
        :rtype: list
        """
        with self.consumers_lock:
            if self.consumers is not None:
                return self.consumers[:]
            return None
