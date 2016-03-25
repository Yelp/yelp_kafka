import logging
import os
import signal
import time
import traceback
from multiprocessing import Event
from multiprocessing import Lock
from multiprocessing import Process
from Queue import Queue
from threading import Thread

import yelp_meteorite
from kafka import KafkaConsumer
from kafka.common import ConsumerTimeout
from kafka.common import KafkaUnavailableError
from yelp_lib.decorators import retry

from yelp_kafka import metrics
from yelp_kafka.consumer import KafkaSimpleConsumer
from yelp_kafka.error import ConsumerGroupError
from yelp_kafka.error import PartitionerError
from yelp_kafka.error import PartitionerZookeeperError
from yelp_kafka.error import ProcessMessageError
from yelp_kafka.partitioner import Partitioner


DEFAULT_REFRESH_TIMEOUT_IN_SEC = 0.5
CONSUMER_GROUP_INTERNAL_TIMEOUT = 100  # milliseconds


class ConsumerGroup(object):
    """Single process consumer group.
    Support partitions distribution for a single topic
    between many consumer instances.

    If the topic consists of only one partition, only one consumer belonging
    to the group will be able to consume messages. The other consumers will
    stay idle, ready to take over the active consumer in case of failures.
    If the topic consists of many partitions and only one consumer has
    joined the group, the consumer will consume messages
    from all the partitions. Finally if the topic consists of many
    partitions and there are many consumers, each consumer will pick up
    a subset of partitions and consume from them.

    .. warning::
        Messages will be named tuples with fields "partition",
        "offset", "key", "value". This message format is different from those
        yielded by :py:class:`yelp_kafka.consumer_group.KafkaConsumerGroup`.

    .. warning:: This class is considered deprecated in favor of :py:class:`yelp_kafka.consumer_group.KafkaConsumerGroup`.

    Example:

    .. code-block:: python

       from yelp_kafka import discovery
       from yelp_kafka.config import KafkaConsumerConfig
       from yelp_kafka.consumer_group import ConsumerGroup

       cluster = discovery.get_local_cluster('standard')
       config = KafkaConsumerConfig('my_group', cluster)

       def my_process_function(message):
           partition, offset, key, value = message
           print partition, offset, key, value

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
            the partitioner many times upon changes.
            See :py:mod:`yelp_kafka.config`
            Default: 5 seconds.
        """
        with self.partitioner:
            while True:
                self.consume(refresh_timeout)

    def consume(self, refresh_timeout):
        """Consume messages from kafka and refresh the group
        upon timeout expiration.

        :param refresh_timeout: refresh period for consumer group
        """
        timeout = time.time() + refresh_timeout
        if self.consumer:
            for message in self.consumer:
                try:
                    self.process(message)
                except:
                    trace = traceback.format_exc()
                    self.log.exception(
                        "Error processing message: %s",
                        message,
                    )
                    raise ProcessMessageError(
                        "Error processing message: {0}\n\nException "
                        "caught inside processing message function:\n{1}"
                        .format(
                            message,
                            trace,
                        )
                    )
                if time.time() > timeout:
                    break
        try:
            self.partitioner.refresh()
        except (PartitionerZookeeperError, PartitionerError):
            self.log.exception("Encountered a partitioner error")
            raise

    def _acquire(self, partitions):
        """Create a consumer ready to consume from kafka.

        .. note: This function should only by called by the partitioner.
           The partitioner executes _acquire when the partition group
           changes and the partitions have been acquired.
        """
        if partitions.get(self.topic):
            self.consumer = KafkaSimpleConsumer(
                self.topic,
                self.config,
                partitions[self.topic]
            )
            try:
                # We explicitly catch and log the exception.
                self.consumer.connect()
            except:
                self.log.exception(
                    "Consumer topic %s, partition %s, config %s:"
                    " failed connecting to kafka",
                    self.topic,
                    partitions,
                    self.config,
                )
                raise

    def _release(self, partitions):
        """Release the consumer.

        .. note: This function should only by called by the partitioner.
           The partitioner executes _release when it has to release the
           acquired partitions.
        """
        if self.consumer:
            self.consumer.close()
            self.consumer = None


class KafkaConsumerGroup(object):
    """KafkaConsumerGroup allows you to efficiently consume from all the
    partitions of a topic without having to manually use the
    :py:class:`yelp_kafka.partitioner.Partitioner` class. You can spin up
    multiple KafkaConsumerGroups, and they will co-ordinate via the Partitioner
    to divvy up the available partitions between each other.

    This class works by attempting to rebalance before each call to `next()`. In
    the event that rebalancing does occur and that you have enabled
    auto-committing, any messages marked as done using `task_done()` will be
    committed before repartitioning. To commit messages immediately, you can
    call `commit()`.

    If metrics_reporter is enabled in config, the metrics prefix in SignalFx
    will be: "yelp_kafka.KafkaConsumerGroup."

    .. warning::
        Do not create multiple KafkaConsumerGroups in the same process;
        the Partitioner class does not work if there are multiple instances of it in
        the same process.

    .. warning::
        Messages will be instances of kafka-python KafkaMessage, which is
        a named tuple with the fields "topic", "partition", "offset", "key",
        "value". This message format is different from those yielded by
        :py:class:`yelp_kafka.consumer_group.ConsumerGroup`.

    Example:

    .. code-block:: python

        from yelp_kafka import discovery
        from yelp_kafka.consumer_group import KafkaConsumerGroup
        from yelp_kafka.config import KafkaConsumerConfig

        cluster = discovery.get_local_cluster('standard')
        config = KafkaConsumerConfig('my_group', cluster)

        # A "tail" consumer that reads, prints, and eventually commits every
        # message from a list of topics.
        consumer = KafkaConsumerGroup(['my-topic1', 'my-topic2'], config)
        with consumer:
            for message in consumer:
                print message.value
                consumer.task_done(message)

    :param topics: a list of topics to consume from.
    :type topics: list
    :param config: yelp_kakfa consumer config.
    :type config: :py:class:`yelp_kafka.config.KafkaConsumerConfig`
    """

    METRIC_PREFIX = 'yelp_kafka.KafkaConsumerGroup'

    def __init__(self, topics, config):
        assert isinstance(topics, list), "Topics must be a list"

        self.log = logging.getLogger(self.__class__.__name__)
        self.topics = topics
        self.partitioner = Partitioner(
            config,
            topics,
            self._acquire,
            self._release
        )
        self.consumer = None
        self.metrics_queue = Queue()
        self.metrics_reporter = None

        consumer_config = config.get_kafka_consumer_config()
        self.log.debug(
            "Using config: %s",
            consumer_config,
        )

        if config.metrics_reporter == 'signalfx':
            self._setup_signalfx_reporter(config)
            consumer_config['metrics_responder'] = self._add_to_metrics_queue
        elif config.metrics_reporter == 'yelp_meteorite':
            self._setup_meteorite_reporter(config)
            consumer_config['metrics_responder'] = self._send_to_yelp_meteorite

        self.pre_rebalance_callback = config.pre_rebalance_callback
        self.post_rebalance_callback = config.post_rebalance_callback

        # Intercept the user's timeout and pass in our own instead. We do this
        # in order to periodically refresh the partitioner when calling next()
        self.iter_timeout = consumer_config['consumer_timeout_ms']
        consumer_config['consumer_timeout_ms'] = CONSUMER_GROUP_INTERNAL_TIMEOUT
        self.config = consumer_config

    def _setup_signalfx_reporter(self, config):
        self.metrics_reporter = metrics.MetricsReporter(
            self.METRIC_PREFIX,
            self.metrics_queue,
            config
        )
        Thread(target=self.metrics_reporter.main_loop).start()

    def _setup_meteorite_reporter(self, config):
        extra_dimensions = config.signalfx_dimensions
        self.timers = {}
        for name in metrics.TIME_METRIC_NAMES:
            topic_name = self.METRIC_PREFIX + '.' + name
            timer = yelp_meteorite.create_timer(topic_name, extra_dimensions)
            self.timers[name] = timer

        self.counters = {}
        for name in metrics.FAILURE_COUNT_METRIC_NAMES:
            topic_name = self.METRIC_PREFIX + '.' + name
            counter = yelp_meteorite.create_counter(topic_name, extra_dimensions)
            self.counters[name] = counter

    def _add_to_metrics_queue(self, key, value):
        self.metrics_queue.put((key, value))

    def _send_to_yelp_meteorite(self, key, value):
        if key in self.timers:
            # kafka-python emits time in seconds, but yelp_meteorite wants
            # milliseconds
            time_in_ms = value * 1000
            self.timers[key].record(time_in_ms)
        elif key in self.counters:
            self.counters[key].count()
        else:
            self.log.warn("Unknown metric: {0}".format(key))

    def start(self):
        self.partitioner.start()

    def stop(self):
        self.partitioner.stop()
        self.consumer.close()

        if self.metrics_reporter:
            self.metrics_reporter.die_event.set()

    def next(self):
        start_time = time.time()
        while self._should_keep_trying(start_time):
            self.partitioner.refresh()
            try:
                return self.consumer.next()
            except ConsumerTimeout:
                # This is due to the internal timeout, not the user's provided
                # one.
                pass
        error_msg = "KafkaConsumerGroup timed out after {0} ms"
        raise ConsumerTimeout(error_msg.format(self.iter_timeout))

    def _should_keep_trying(self, start_time):
        if self.iter_timeout < 0:
            return True
        elapsed_seconds = time.time() - start_time
        return elapsed_seconds * 1000 < self.iter_timeout

    def task_done(self, message):
        return self.consumer.task_done(message)

    def commit(self):
        return self.consumer.commit()

    def _acquire(self, partitions):
        if not self.consumer:
            self.consumer = KafkaConsumer(partitions, **self.config)
        else:
            self.consumer.set_topic_partitions(partitions)
        if self.post_rebalance_callback:
            self.post_rebalance_callback(partitions)

    # set_topic_partitions causes a metadata request, which may fail on the
    # first try.
    @retry(2, exceptions=(KafkaUnavailableError,))
    def _release(self, partitions):
        if self.pre_rebalance_callback:
            self.pre_rebalance_callback(partitions)
        if self._auto_commit_enabled():
            self.consumer.commit()
        self.consumer.set_topic_partitions({})

    def _auto_commit_enabled(self):
        return self.config['auto_commit_enable']

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.stop()

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()


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
       from yelp_kafka import discovery
       from yelp_kafka.config import KafkaConsumerConfig
       from yelp_kafka.consumer import KafkaConsumer
       from yelp_kafka.consumer_group import MultiprocessingConsumerGroup

       class MyConsumer(KafkaConsumerBase):

           def __init__(topic, config, partitions):
               super(MyConsumer, self).__init__(topic, config, partitions)

           def initialize(self):
               print "Initializing.. Usually you want to set the logger here"

           def dispose(self):
               print "Dying..."

           def process(self, message):
               partition, offset, key, value = message
               print partition, offset, key, value

       cluster = discovery.get_local_cluster('standard')
       config = KafkaConsumerConfig('my_group', cluster)

       group = MultiprocessingConsumerGroup(
           ['topic1', 'topic2'],
           config,
           MyConsumer,
       )
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
        :py:class:`yelp_kafka.consumer.KafkaConsumerBase`. It has to return
        an instance of a subclass of
        :py:class:`yelp_kafka.consumer.KafkaConsumerBase`.
    """

    def __init__(self, topics, config, consumer_factory):
        self.config = config
        self.termination_flag = None
        self.partitioner = Partitioner(
            config,
            topics,
            self.acquire,
            self.release,
        )
        self.consumers = None
        self.consumers_lock = Lock()
        self.consumer_procs = {}
        self.consumer_factory = consumer_factory
        self.log = logging.getLogger(self.__class__.__name__)
        self.pre_rebalance_callback = config.pre_rebalance_callback
        self.post_rebalance_callback = config.post_rebalance_callback

    def start_group(self, refresh_timeout=DEFAULT_REFRESH_TIMEOUT_IN_SEC):
        """Start the consumer group.

        :param refresh_timeout: waiting timeout in secs to refresh
            the consumer group. It should never be greater than
            'zk_partitioner_cooldown' to avoid wasting time resetting
            the partitioner many times upon changes.
            See :py:mod:`yelp_kafka.config`
            Default: 5 seconds.

        .. note: this function does not return. You may want to run it into
            a separate thread.

        """
        # Create the termination flag
        self.termination_flag = Event()

        with self.partitioner:
            while not self.termination_flag.is_set():
                self.termination_flag.wait(refresh_timeout)
                self.monitor()
                try:
                    self.partitioner.refresh()
                except (PartitionerZookeeperError, PartitionerError):
                    self.log.exception("Encountered a partitioner error")
                    raise

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
        if self.post_rebalance_callback:
            self.post_rebalance_callback(partitions)

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
        try:
            proc = Process(
                target=consumer.run,
                name='Consumer-{0}-{1}'.format(
                    consumer.topic,
                    consumer.partitions,
                ),
            )
            proc.daemon = True
            proc.start()
        except Exception:
            self.log.error(
                "Impossible to start a new consumer."
                "Topic: %s. Partition: %s.",
                consumer.topic,
                consumer.partitions,
            )
            raise ConsumerGroupError(
                "Error starting a new consumer."
                "Topic: {topic}. Partition: {partitions}.".format(
                    topic=consumer.topic,
                    partitions=consumer.partitions,
                ),
            )
        return proc

    def release(self, partitions):
        """Terminate all the consumer processes"""
        if self.pre_rebalance_callback:
            self.pre_rebalance_callback(partitions)
        self.log.info("Terminating consumer group")
        for consumer in self.consumer_procs.itervalues():
            consumer.terminate()

        timeout = time.time() + self.config.max_termination_timeout_secs
        while (
            time.time() <= timeout and any(
                [proc.is_alive() for proc in self.consumer_procs.iterkeys()]
            )
        ):
            continue

        for proc, consumer in self.consumer_procs.iteritems():
            if proc.is_alive():
                os.kill(proc.pid, signal.SIGKILL)
                self.log.error(
                    "Process %s, topic %s, partitions %s:"
                    "killed due to timeout",
                    proc.name,
                    consumer.topic,
                    consumer.partitions,
                )
        self.consumer_procs.clear()
        with self.consumers_lock:
            self.consumers = None

    def monitor(self):
        """Respawn consumer processes upon failures."""
        # We don't use the iterator because the dict may change during the loop
        for proc, consumer in self.consumer_procs.items():
            if not proc.is_alive():
                self.log.error(
                    "consumer process %s topic %s partitions %s: "
                    "died exit status %s",
                    proc.name,
                    consumer.topic,
                    consumer.partitions, proc.exitcode,
                )
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
