import logging
from multiprocessing import Event
from multiprocessing import Lock
from multiprocessing import Process
import time
import os
import signal

from yelp_kafka.error import ConsumerGroupError
from yelp_kafka.error import ProcessMessageError
from yelp_kafka.partitioner import Partitioner
from yelp_kafka.consumer import KafkaSimpleConsumer


DEFAULT_REFRESH_TIMEOUT_IN_SEC = 0.5


class ConsumerGroup(object):
    """Single process consumer group.
    Support partitions distribution for a single topic
    between many consumer instances.

    If the topic consists of only one partition, only one consumer belonging
    to the group will be able to consume messages. The other consumers will
    stay idle, ready to take over the active consumer in case of failures.
    If the topic consists of many partitions and only one consumer has
    joined the group, the consumer will consume messages from all the partitions.
    Finally if the topic consists of many partitions and there are many consumers,
    each consumer will pick up a subset of partitions and consume from them.

    Example:

    .. code-block:: python

       def my_process_function(message):
           partition, offset, key, value = message
           print partition, offset, key, value

       config = {'brokers': ['kafka_host:9092'], 'zk_hosts': ['zookeeper:2181'],
                 'group_id': 'my_group'}
       consumer = ConsumerGroup('test_topic', config, my_process_function)
       consumer.run()

    :param topics: topics to consume from
    :type topics: list
    :param config: yelp_kakfa config. See :py:mod:`yelp_kafka.config`
    :type config: dict
    :param process_func: function used to process the message.
        This function should accept a :py:data:`yelp_kafka.consumer.Message`
        as parameter.
    """

    def __init__(self, topic, config, process_func):
        self.log = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.topic = topic
        self.partitioner = Partitioner(
            config,
            topic if isinstance(topic, list) else [topic],
            self._acquire,
            self._release
        )
        self.consumer = None
        self.process = process_func

    def run(self, refresh_timeout=DEFAULT_REFRESH_TIMEOUT_IN_SEC):
        """Create the group, instantiate a consumer and consume message
        from kafka. The refresh timeout shouldn't be too high in order to
        allow the consumers to stay updated with the group changes.

        :param refresh_timeout: waiting timeout in secs to refresh
            the consumer group. It should never be greater than
            'zk_partitioner_cooldown' to avoid wasting time resetting
            the partitioner many times upon changes. See :py:mod:`yelp_kafka.config`
            Default: 5 seconds.
        """
        self.partitioner.start()
        while True:
            self._consume(refresh_timeout)

    def _consume(self, refresh_timeout):
        """Consume messages from kafka and refresh the group
        upon timeout expiration.

        :param refresh_timeout: refresh period for consumer group
        """
        timeout = time.time() + refresh_timeout
        for message in self.consumer:
            try:
                self.process(message)
            except:
                self.log.exception("Error processing message: %s", message)
                raise ProcessMessageError("Error processing message: %s", message)
            if time.time() > timeout:
                break
        self.partitioner.refresh()

    def _acquire(self, partitions):
        """Create a consumer ready to consume from kafka.

        .. note: This function should only by called by the partitioner.
           The partitioner executes _acquire when the partition group
           changes and the partitions have been acquired.
        """
        self.consumer = KafkaSimpleConsumer(self.topic, self.config,
                                            partitions[self.topic])
        try:
            # We explicitly catch and log the exception.
            self.consumer.connect()
        except:
            self.log.exception("Consumer topic %s, partition %s, config %s:"
                               " failed connecting to kafka", self.topic,
                               partitions, self.config)
            raise

    def _release(self, partitions):
        """Release the consumer.

        .. note: This function should only by called by the partitioner.
           The partitioner executes _release when it has to release the
           acquired partitions.
        """
        self.consumer.close()
        self.consumer = None


class MultiprocessingConsumerGroup(object):
    """Multiprocessing consumer group allows to consume
    from multiple topics at once. It spawns a python process
    for each assigned partition.
    It also implements monitoring for the running consumers and
    is able to restart restart these upon failures.

    .. note: This class is thread safe.

    Example:

    .. code-block:: python

       from threading import Thread
       from yelp_kafka.consumer import KafkaConsumer
       from yelp_kafka.consumer_group import MultiprocessingConsumerGroup

       class MyConsumer(KafkaConsumer):

           def __init__(topic, config, partitions):
               super(MyConsumer, self).__init__(topic, config, partitions)

           def initialize(self):
               print "Initializing.. Usually you want to set the logger here"

           def dispose(self):
               print "Dying..."

           def process(self, message):
               partition, offset, key, value = message
               print partition, offset, key, value

       group = MultiprocessingConsumerGroup(['topic1', 'topic2'], config, MyConsumer)
       group_thread = Thread(target=group.start_group)
       group_thread.start()
       # Do some other cool stuff here like sleep
       time.sleep(600)
       group.stop_group()

    :param topics: a list of topics to consume from.
    :type topics: list
    :param config: yelp_kakfa config. See :py:mod:`yelp_kafka.config`
    :type config: dict
    :param consumer_factory: the function used to instantiate the consumer.
        the consumer_factory must have the same argument list of
        :py:class:`yelp_kafka.consumer.KafkaConsumer`. It has to return
        an instance of a subclass of :py:class:`yelp_kafka.consumer.KafkaConsumer`.
    """
    def __init__(self, topics, config, consumer_factory):
        self.config = config
        self.termination_flag = None
        self.partitioner = Partitioner(config, topics, self.acquire, self.release)
        self.consumers = None
        self.consumers_lock = Lock()
        self.consumer_procs = {}
        self.consumer_factory = consumer_factory
        self.log = logging.getLogger(self.__class__.__name__)

    def start_group(self, refresh_timeout=DEFAULT_REFRESH_TIMEOUT_IN_SEC):
        """Start the consumer group.

        :param refresh_timeout: waiting timeout in secs to refresh
            the consumer group. It should never be greater than
            'zk_partitioner_cooldown' to avoid wasting time resetting
            the partitioner many times upon changes. See :py:mod:`yelp_kafka.config`
            Default: 5 seconds.

        .. note: this function does not return. You may want to run it into
            a separate thread.

        """
        # Create the termination flag
        self.termination_flag = Event()
        self.partitioner.start()
        while not self.termination_flag.is_set():
            self.termination_flag.wait(refresh_timeout)
            self.monitor()
            self.partitioner.refresh()
        # Release the group for termination
        self.partitioner.stop()

    def stop_group(self):
        """Set the termination flag to stop the group.

        :raises: ConsumerGroupError is the group has not been started, yet.
        """
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
                    " partition = %s", topic, self.config, p
                )
                consumer = self.consumer_factory(topic, self.config, [p])
                self.consumer_procs[self.start_consumer(consumer)] = consumer
        return self.consumer_procs.values()

    def start_consumer(self, consumer):
        """Create a new consumer process"""
        proc = Process(target=consumer.run, name='Consumer-{0}-{1}'.format(consumer.topic, consumer.partitions))
        proc.start()
        return proc

    def release(self, partitions):
        """Terminate all the consumer processes"""
        self.log.warning("Terminating consumer group")
        for consumer in self.consumer_procs.itervalues():
            consumer.terminate()

        timeout = time.time() + self.config.max_termination_timeout_secs
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
                self.consumer_procs[self.start_consumer(consumer)] = consumer
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
