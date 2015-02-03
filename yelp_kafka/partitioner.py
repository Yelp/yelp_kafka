from collections import defaultdict
import logging
import time

from kafka import KafkaClient
from kafka.common import KafkaUnavailableError
from kazoo.client import KazooClient
from kazoo.protocol.states import KazooState
from kazoo.recipe.partitioner import PartitionState

from yelp_kafka.config import load_config_or_default
from yelp_kafka.error import PartitionerError

MAX_START_TIME_SECS = 30


class Partitioner(object):
    """Partitioner is used to handle distributed a set of topics/partitions among
    a group of consumers.

    :param topics: kafka topics
    :type topics: list
    :param acquire: function to be called when a set of partitions has been acquired.
        It should usually allocate the consumers.
    :type acquire: func
    :param release: function to be called when the acquired
        partitions have to be release. It should usually stops the consumers.
    """
    def __init__(self, config, topics, acquire, release):
        self.kafka_hosts = ','.join(config['brokers'])
        self.kazooclient = KazooClient(','.join(config['zk_hosts']))
        self.topics = topics
        self.acquired_partitions = defaultdict(list)
        self.partitions_set = None
        self.acquire = acquire
        self.release = release
        self._config = load_config_or_default(config)
        self._partitioner = None
        self.log = logging.getLogger(self.__class__.__name__)
        self.group_actions = {
            PartitionState.ALLOCATING: self._allocating,
            PartitionState.ACQUIRED: self._acquire,
            PartitionState.RELEASE: self._release,
            PartitionState.FAILURE: self._fail
        }

    def start(self):
        """Create a new group and wait until the partitions have been
        acquired.

        .. note: This is a blocking operation.
        """
        self.log.debug("Starting a new group for topics %s", self.topics)
        self._refresh()

    def stop(self):
        """Leave the group and release the partitions."""
        self.log.debug("Stopping group for topics %s", self.topics)
        self._destroy_partitioner(self._partitioner)

    def refresh(self):
        """Rebalance upon group changes, such as when a consumer
        joins/leaves the group, the partitions for a topics change, or the
        partitioner itself fails (connection to zookeeper lost).
        This method should be called periodically to make sure that the
        group is in sync.
        """
        self.log.debug("Refresh group for topics %s", self.topics)
        self._refresh()

    def get_group_path(self):
        """Get the group path in zookeeper."""
        return '/'.join([self._config['zookeeper_base'],
                        self._config['group_id']])

    def _refresh(self):
        while True:
            partitions = self.get_partitions_set()
            partitioner = self._get_partitioner(partitions)
            self._handle_group(self, partitioner)
            if self.acquired_partitions:
                break

    def _get_partitioner(self, partitions):
        """Get an instance of the partitioner. When the partitions set changes
         we need to destroy the partitioner and create another one.
        If the partitioner does not exist yet, create a new partitioner.
        If the partitions set changed, destroy the partitioner and create a new
        partitioner. Different consumer will eventually use the same partitions set.

        :param partitions: the partitions set to use for partitioner.
        :type partitions: set
        """
        if not self.partitions_set or not self._partitioner:
            self._partitioner = self._create_partitioner(partitions)
            self.partitions_set = partitions
        elif partitions != self.partitions_set:
            # If partitions changed we release the consumers, destroy the
            # partitioner and disconnect from zookeeper.
            self._destroy_partitioner(self._partitioner)
            # Wait for the group to settle on the new partitions set before
            # creating a new partitioner.
            time.sleep(self._config['zk_partitioner_cooldown'])
            self._partitioner = self._create_partitioner(partitions)
            self.partitions_set = partitions
        return self._partitioner

    def _create_partitioner(self, partitions):
        """Connect to zookeeper and create a partitioner"""
        if self.kazooclient.state != KazooState.CONNECTED:
            try:
                self.kazooclient.start()
            except:
                self.log.exception("Impossible to connect to zookeeper")
                raise PartitionerError("Zookeeper connection failure")
        self.log.debug("Creating partitioner for group %s, topic %s,"
                       " partitions set %s", self._config['group_id'],
                       self.topics, partitions)
        return self.kazooclient.SetPartitioner(
            path=self.get_group_path(),
            set=partitions,
            time_boundary=self._config['zk_partitioner_cooldown']
        )

    def _destroy_partitioner(self, partitioner):
        """Release consumers and terminate the partitioner"""
        if not partitioner:
            raise PartitionerError("Internal error partitioner not yet started.")
        self._release(partitioner)
        partitioner.finish()
        self.kazooclient.stop()

    def _handle_group(self, partitioner):
        """Handle group status changes, for example when a new
        consumer joins or leaves the group.
        """
        if partitioner:
            try:
                self.group_actions[partitioner.state](partitioner)
            except KeyError:
                self.log.exception("Unexpected partitioner state.")
                raise PartitionerError("Invalid partitioner state %s" %
                                       partitioner.state)

    def _allocating(self, partitioner):
        """Usually we don't want to do anything but waiting in
        allocating state.
        """
        partitioner.wait_for_acquire()

    def _acquire(self, partitioner):
        """Acquire kafka topics-[partitions] and start the
        consumers for them.
        """
        if not self.acquired_partitions:
            self.acquired_partitions = self._get_acquired_partitions(partitioner)
            self.log.debug("Acquired partitions: %s", self.acquired_partitions)
            self.acquire(self.acquired_partitions)

    def _release(self, partitioner):
        """Release the consumers and acquired partitions.
        This function is executed either at termination time or
        whenever there is a group change.
        """
        self.log.warning("Releasing partitions")
        self.release(self.acquired_partitions)
        partitioner.release_set()
        self.acquired_partitions.clear()

    def _fail(self, partitioner):
        """Handle zookeeper failures.
        Executed when the consumer group is not able to recover
        the connection. In this case, we cowardly stop
        the running consumers.
        """
        self.log.error("Lost or unable to acquire partitions")
        if self.acquired_partitions:
            self.release(self.acquired_partitions)
            self.acquired_partitions.clear()
            # The partitioner is in fail state so we can get rid of it and try
            # to create a new one.
            self.partitions_set = None
            self._partitioner = None

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

    def get_partitions_set(self):
        """ Load partitions metadata from kafka and create
        a set containing "<topic>-<partition_id>"

        :returns: partitions for user topics
        :rtype: set
        """

        kafkaclient = KafkaClient(
            self._config['brokers'],
            client_id=self._config['client_id']
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
        kafkaclient.close()
        return set(partitions)
