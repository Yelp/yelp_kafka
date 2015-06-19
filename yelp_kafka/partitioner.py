from collections import defaultdict
import hashlib
import logging
import time

from kafka.client import KafkaClient
from kazoo.client import KazooClient
from kazoo.protocol.states import KazooState
from kazoo.recipe.partitioner import PartitionState
from kazoo.retry import KazooRetry

from yelp_kafka.error import PartitionerError, PartitionerZookeeperError
from yelp_kafka.utils import get_kafka_topics

MAX_START_TIME_SECS = 300
# The java kafka api updates every 600s by default. We update the
# number of partitions every 120 seconds.
PARTITIONS_REFRESH_TIMEOUT = 120

# Define the connection retry policy for kazoo in case of flaky
# zookeeper connections. This ensures we don't keep indefinitely
# trying to connect and masking failures from the application.
KAZOO_RETRY_DEFAULTS = {
    'max_tries': 10,
    'delay': 0.1,
    'backoff': 2,
    'max_jitter': 0.8,
    'max_delay': 60,
}


def build_zk_group_path(group_path, topics):
    return "{group_path}/{sha}".format(
        group_path=group_path,
        sha=hashlib.sha1(repr(sorted(topics))).hexdigest(),
    )


class Partitioner(object):
    """Partitioner is used to handle distributed a set of
    topics/partitions among a group of consumers.

    :param topics: kafka topics
    :type topics: list
    :param acquire: function to be called when a set of partitions
    has been acquired.
        It should usually allocate the consumers.
    :type acquire: func
    :param release: function to be called when the acquired
        partitions have to be release. It should usually stops the consumers.
    """
    def __init__(self, config, topics, acquire, release):
        self.config = config
        self.kazoo_client = None
        self.kafka_client = None
        self.topics = topics
        self.acquired_partitions = defaultdict(list)
        self.partitions_set = set()
        self.acquire = acquire
        self.release = release
        self.config = config
        self.force_partitions_refresh = True
        self.last_partitions_refresh = 0
        self._partitioner = None
        self.log = logging.getLogger(self.__class__.__name__)
        self.actions = {
            PartitionState.ALLOCATING: self._allocating,
            PartitionState.ACQUIRED: self._acquire,
            PartitionState.RELEASE: self._release,
            PartitionState.FAILURE: self._fail
        }
        self.kazoo_retry = None
        self.zk_group_path = build_zk_group_path(
            self.config.group_path,
            self.topics
        )

    def start(self):
        """Create a new group and wait until the partitions have been
        acquired.

        :raises: PartitionerError upon partitioner failures

        .. note: This is a blocking operation.
        """
        self.kazoo_retry = KazooRetry(**KAZOO_RETRY_DEFAULTS)
        self.kazoo_client = KazooClient(
            self.config.zookeeper,
            connection_retry=self.kazoo_retry,
        )
        self.kafka_client = KafkaClient(self.config.broker_list)
        self.log.debug("Starting a new group for topics %s", self.topics)
        self._refresh()

    def stop(self):
        """Leave the group and release the partitions."""
        self.log.debug("Stopping group for topics %s", self.topics)
        self.release_and_destroy()

    def refresh(self):
        """Rebalance upon group changes, such as when a consumer
        joins/leaves the group, the partitions for a topics change, or the
        partitioner itself fails (connection to zookeeper lost).
        This method should be called periodically to make sure that the
        group is in sync.

        :raises: PartitionerError upon partitioner failures
        """
        self.log.debug("Refresh group for topics %s", self.topics)
        self._refresh()

    def _refresh(self):
        while True:
            partitioner = self._get_partitioner()
            self._handle_group(partitioner)
            if self.acquired_partitions:
                break

    def need_partitions_refresh(self):
        return (self.force_partitions_refresh or
                self.last_partitions_refresh <
                time.time() - PARTITIONS_REFRESH_TIMEOUT)

    def _get_partitioner(self):
        """Get an instance of the partitioner. When the partitions set changes
         we need to destroy the partitioner and create another one.
        If the partitioner does not exist yet, create a new partitioner.
        If the partitions set changed, destroy the partitioner and create a new
        partitioner. Different consumer will eventually use
        the same partitions set.

        :param partitions: the partitions set to use for partitioner.
        :type partitions: set
        """
        if self.need_partitions_refresh() or not self._partitioner:
            try:
                partitions = self.get_partitions_set()
            except:
                self.log.exception(
                    "Failed to get partitions set from Kafka."
                    "Releasing the group."
                )
                self.release_and_destroy()
                raise PartitionerError("Failed to get partitions set from Kafka")
            self.force_partitions_refresh = False
            self.last_partitions_refresh = time.time()
            if partitions != self.partitions_set:
                # If partitions changed we release the consumers, destroy the
                # partitioner and disconnect from zookeeper.
                self.log.info(
                    "Partitions set changed. New partitions: %s. "
                    "Old partitions %s. Rebalancing...",
                    [p for p in partitions if p not in self.partitions_set],
                    [p for p in self.partitions_set if p not in partitions]
                )
                # We need to destroy the existing partitioner before creating
                # a new one.
                self.release_and_destroy()
                self._partitioner = self._create_partitioner(partitions)
                self.partitions_set = partitions
        return self._partitioner

    def _create_partitioner(self, partitions):
        """Connect to zookeeper and create a partitioner"""
        if self.kazoo_client.state != KazooState.CONNECTED:
            try:
                self.kazoo_client.start()
            except:
                self.log.exception("Impossible to connect to zookeeper")
                self.release_and_destroy()
                raise PartitionerError("Zookeeper connection failure")

        self.log.debug("Creating partitioner for group %s, topic %s,"
                       " partitions set %s", self.config.group_id,
                       self.topics, partitions)
        return self.kazoo_client.SetPartitioner(
            path=self.zk_group_path,
            set=partitions,
            time_boundary=self.config.partitioner_cooldown,
        )

    def release_and_destroy(self):
        """Release consumers and terminate the partitioner"""
        if self._partitioners:
            self._release(self._partitioner)
            self._partitioner.finish()
        self._close_connections()
        self._partitioner = None

    def _close_connections(self):
        self.kafka_client.close()
        self.partitions_set = set()
        self.last_partitions_refresh = 0
        self.kazoo_client.stop()
        self.kazoo_client.close()
        self.kazoo_retry = None

    def _handle_group(self, partitioner):
        """Handle group status changes, for example when a new
        consumer joins or leaves the group.
        """
        if partitioner:
            try:
                self.actions[partitioner.state](partitioner)
            except KeyError:
                self.log.exception("Unexpected partitioner state.")
                self.release_and_destroy()
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
        acquired_partitions = self._get_acquired_partitions(partitioner)
        if acquired_partitions != self.acquired_partitions:
            # TODO: Decrease logging level
            self.log.info(
                "Total number of acquired partitions = %s"
                "It was %s before. Added partitions %s. Removed partitions %s",
                len(acquired_partitions),
                len(self.acquired_partitions),
                [
                    p for p in acquired_partitions
                    if p not in self.acquired_partitions
                ],
                [
                    p for p in self.acquired_partitions
                    if p not in acquired_partitions
                ],
            )
            self.acquired_partitions = acquired_partitions
            try:
                self.acquire(self.acquired_partitions)
            except Exception:
                self.log.exception("Acquire action failed.")
                self.release_and_destroy()
                raise PartitionerError("Acquire action failed.")

    def _release(self, partitioner):
        """Release the consumers and acquired partitions.
        This function is executed either at termination time or
        whenever there is a group change.
        """
        self.log.debug("Releasing partitions")
        try:
            self.release(self.acquired_partitions)
        except Exception:
            self.log.exception("Acquire action failed.")
            self._destroy_partitioner()
            raise PartitionerError("Acquire action failed.")
        partitioner.release_set()
        self.acquired_partitions.clear()
        self.force_partitions_refresh = True

    def _fail(self, partitioner):
        """Handle zookeeper failures.
        Executed when the consumer group is not able to recover
        the connection. In this case, we cowardly stop
        the running consumers.
        """
        self.log.error("Lost or unable to acquire partitions")
        self.release_and_destroy()
        raise PartitionerZookeeperError

    def _get_acquired_partitions(self, partitioner):
        """Retrieve acquired partitions from a partitioner.

        :returns: acquired topic and partitions
        :rtype: dict {<topic>: <[partitions]>}
        """
        acquired_partitions = defaultdict(list)
        for partition in partitioner:
            topic, partition_id = partition.rsplit('-', 1)
            acquired_partitions[topic].append(int(partition_id))
        return acquired_partitions

    def get_partitions_set(self):
        """ Load partitions metadata from kafka and create
        a set containing "<topic>-<partition_id>"

        :returns: partitions for user topics
        :rtype: set
        :raises PartitionerError: if no partitions have been found
        """
        topic_partitions = get_kafka_topics(self.kafka_client)
        partitions = []
        missing_topics = set()
        for topic in self.topics:
            if topic not in topic_partitions:
                missing_topics.add(topic)
            else:
                partitions += ["{0}-{1}".format(topic, p)
                               for p in topic_partitions[topic]]
        if missing_topics:
            self.log.info("Missing topics: %s", missing_topics)
        if not partitions:
            self.release_and_destroy()
            raise PartitionerError(
                "No partitions found for topics: {topics}".format(
                    topics=self.topics
                )
            )
        return set(partitions)
