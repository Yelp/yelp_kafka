from collections import defaultdict
from collections import namedtuple
import logging

from kafka.common import (
    BrokerResponseError,
    OffsetFetchRequest,
    OffsetRequest,
    OffsetFetchResponse,
    OffsetResponse,
    UnknownTopicOrPartitionError,
    check_error,
    KafkaUnavailableError,
)
from kafka.util import kafka_bytestring

from yelp_kafka.error import (
    UnknownPartitions,
    UnknownTopic,
)


ConsumerPartitionOffsets = namedtuple('ConsumerPartitionOffsets',
                                      ['topic', 'partition', 'current',
                                       'highmark', 'lowmark'])
"""Tuple representing the consumer offsets for a topic partition.

* **topic**\(``str``): Name of the topic
* **partition**\(``int``): Partition number
* **current**\(``int``): current group offset
* **highmark**\(``int``): high watermark
* **lowmark**\(``int``): low watermark
"""

PartitionOffsets = namedtuple('PartitionOffsets',
                              ['topic', 'partition',
                               'highmark', 'lowmark'])
"""Tuple representing the offsets for a topic partition.

* **topic**\(``str``): Name of the topic
* **partition**\(``int``): Partition number
* **highmark**\(``int``): high watermark
* **lowmark**\(``int``): low watermark
"""

log = logging.getLogger(__name__)


def pluck_topic_offset_or_zero_on_unknown(resp):
    try:
        check_error(resp)
    except UnknownTopicOrPartitionError:
        # If the server doesn't have any commited offsets by this group for
        # this topic, assume it's zero.
        log.exception("Error in OffsetFetchResponse")
        pass
    # The API spec says server wont set an error, but 0.8.1.1 does. The actual
    # check is if the offset is -1.
    if resp.offset == -1:
        return OffsetFetchResponse(
            resp.topic,
            resp.partition,
            0,
            resp.metadata,
            0,
        )
    return resp


def check_response_error(resp):
    try:
        check_error(resp)
    except BrokerResponseError:
        # In case of error we set the offset to -1
        log.exception("Error in OffsetResponse")
        return OffsetResponse(
            resp.topic,
            resp.partitions,
            resp.error,
            -1,
        )
    return resp


def _validate_topics_list_or_dict(topics):
    if isinstance(topics, dict):
        return topics
    elif isinstance(topics, (list, set, tuple)):
        return dict([(topic, []) for topic in topics])
    else:
        raise TypeError("Invalid topics: {topics}. It must be either a "
                        "list of topics or a dict "
                        "topic: [partitions]".format(topics=topics))


def _verify_topics_and_partitions(kafka_client, topics, fail_on_error):
    topics = _validate_topics_list_or_dict(topics)
    valid_topics = {}
    for topic, partitions in topics.iteritems():
        # Check topic exists
        if not kafka_client.has_metadata_for_topic(topic):
            if fail_on_error:
                raise UnknownTopic("Topic {topic!r} does not exist in "
                                   "kafka".format(topic=topic))
            else:
                continue
        if partitions:
            # Check the partitions really exist
            unknown_partitions = set(partitions) - \
                set(kafka_client.get_partition_ids_for_topic(topic))
            if unknown_partitions:
                if fail_on_error:
                    raise UnknownPartitions(
                        "Partitions {partitions!r} for topic {topic!r} do not"
                        "exist in kafka".format(
                            partitions=unknown_partitions,
                            topic=topic,
                        )
                    )
                else:
                    # We only use the available partitions in this case
                    partitions = set(partitions) - unknown_partitions
        else:
            # Default get all partitions metadata
            partitions = kafka_client.get_partition_ids_for_topic(topic)
        valid_topics[topic] = partitions
    return valid_topics


def get_current_consumer_offsets(kafka_client, group, topics,
                                 fail_on_error=True):
    """ Get current consumer offsets.

    NOTE: This method does not refresh client metadata. It is up to the caller
    to avoid using stale metadata.

    :param kafka_client: a connected KafkaClient
    :param group: kafka group_id
    :param topics: topic list or dict {<topic>: [partitions]}
    :param fail_on_error: if False the method ignore missing topics and
    missing partitions.
    :returns: a dict topic: partition: offset
    It still may fail on the request send. For example if any partition
    leader is not available the request fails for all the other topics.
    This is the tradeoff of sending all topic requests in batch and save
    both in performance and Kafka load.
    :raises :py:class:`yelp_kafka.error.UnknownTopic`: upon missing
    topics and fail_on_error=True
    :raises :py:class:`yelp_kafka.error.UnknownPartition`: upon missing
    partitions and fail_on_error=True
    :raises FailedPayloadsError: upon send request error.
    """

    topics = _verify_topics_and_partitions(kafka_client, topics, fail_on_error)

    group_offset_reqs = [
        OffsetFetchRequest(kafka_bytestring(topic), partition)
        for topic, partitions in topics.iteritems()
        for partition in partitions
    ]

    group_offsets = {}

    if group_offset_reqs:
        # fail_on_error = False does not prevent network errors
        group_resps = kafka_client.send_offset_fetch_request(
            kafka_bytestring(group),
            group_offset_reqs,
            fail_on_error=False,
            callback=pluck_topic_offset_or_zero_on_unknown,
        )
        for resp in group_resps:
            group_offsets.setdefault(
                resp.topic,
                {},
            )[resp.partition] = resp.offset

    return group_offsets


def get_topics_watermarks(kafka_client, topics, fail_on_error=True):
    """ Get current topic watermarks.

    NOTE: This method does not refresh client metadata. It is up to the caller
    to use avoid using stale metadata.

    :param kafka_client: a connected KafkaClient
    :param topics: topic list or dict {<topic>: [partitions]}
    :param fail_on_error: if False the method ignore missing topics
    and missing partitions.
    :returns: a dict topic: partition: Part
    It still may fail on the request send. For example if any partition
    leader is not available the request fails for all the other topics.
    This is the tradeoff of sending all topic requests in batch and save
    both in performance and Kafka load.
    :raises :py:class:`yelp_kafka.error.UnknownTopic`: upon missing
    topics and fail_on_error=True
    :raises :py:class:`yelp_kafka.error.UnknownPartition`: upon missing
    partitions and fail_on_error=True
    :raises FailedPayloadsError: upon send request error.
    """
    topics = _verify_topics_and_partitions(
        kafka_client,
        topics,
        fail_on_error,
    )
    highmark_offset_reqs = []
    lowmark_offset_reqs = []

    for topic, partitions in topics.iteritems():
        # Batch watermark requests
        for partition in partitions:
            # Request the the latest offset
            highmark_offset_reqs.append(
                OffsetRequest(
                    kafka_bytestring(topic), partition, -1, max_offsets=1
                )
            )
            # Request the earliest offset
            lowmark_offset_reqs.append(
                OffsetRequest(
                    kafka_bytestring(topic), partition, -2, max_offsets=1
                )
            )

    watermark_offsets = {}

    if not (len(highmark_offset_reqs) + len(lowmark_offset_reqs)):
        return watermark_offsets

    # fail_on_error = False does not prevent network errors
    highmark_resps = kafka_client.send_offset_request(
        highmark_offset_reqs,
        fail_on_error=False,
        callback=check_response_error,
    )
    lowmark_resps = kafka_client.send_offset_request(
        lowmark_offset_reqs,
        fail_on_error=False,
        callback=check_response_error,
    )

    # At this point highmark and lowmark should ideally have the same length.
    assert len(highmark_resps) == len(lowmark_resps)
    aggregated_offsets = defaultdict(lambda: defaultdict(dict))
    for resp in highmark_resps:
        aggregated_offsets[resp.topic][resp.partition]['highmark'] = \
            resp.offsets[0]
    for resp in lowmark_resps:
        aggregated_offsets[resp.topic][resp.partition]['lowmark'] = \
            resp.offsets[0]

    for topic, partition_watermarks in aggregated_offsets.iteritems():
        for partition, watermarks in partition_watermarks.iteritems():
            watermark_offsets.setdefault(
                topic,
                {},
            )[partition] = PartitionOffsets(
                topic,
                partition,
                watermarks['highmark'],
                watermarks['lowmark'],
            )
    return watermark_offsets


def get_consumer_offsets_metadata(kafka_client, group,
                                  topics, fail_on_error=True):
    """This method:
        * refreshes metadata for the kafka client
        * fetches group offsets
        * fetches watermarks

    :param kafka_client: KafkaClient instance
    :param group: group id
    :topics: list of topics
    :returns: dict <topic>: [ConsumerPartitionOffsets]
    """

    # Refresh client metadata. We do now use the topic list, because we
    # don't want to accidentally create the topic if it does not exist.
    # If Kafka is unavailable, let's retry loading client metadata (YELPKAFKA-30)
    try:
        kafka_client.load_metadata_for_topics()
    except KafkaUnavailableError:
        kafka_client.load_metadata_for_topics()

    group_offsets = get_current_consumer_offsets(
        kafka_client, group, topics, fail_on_error
    )

    watermarks = get_topics_watermarks(
        kafka_client, topics, fail_on_error
    )

    result = {}
    for topic, partitions in group_offsets.iteritems():
        result[topic] = [
            ConsumerPartitionOffsets(
                topic=topic,
                partition=partition,
                current=group_offsets[topic][partition],
                highmark=watermarks[topic][partition].highmark,
                lowmark=watermarks[topic][partition].lowmark,
            ) for partition in partitions
        ]
    return result


def topics_offset_distance(kafka_client, group, topics):
    """Get the distance a group_id is from the current latest offset
    for topics.

    If the group is unkown to kafka it's assumed to be an offset 0. All other
    errors will not be caught.

    This method force the client to use fresh metadata by calling
    kafka_client.load_metadata_for_topics(topics) before getting
    the group offsets.

    :param kafka_client: KafkaClient instance
    :param topics: topics list or dict <topic>: <[partitions]>
    :param group: consumer group id
    :returns: dict <topic>: {<partition>: <distance>}
    """

    distance = {}
    for topic, offsets in get_consumer_offsets_metadata(
        kafka_client,
        group,
        topics,
    ).iteritems():
        distance[topic] = dict([
            (offset.partition, offset.highmark - offset.current)
            for offset in offsets
        ])
    return distance


def offset_distance(kafka_client, group, topic, partitions=None):
    """Get the distance a group_id is from the current latest in a topic.

    If the group is unknown to kafka, it's assumed to be on offset 0. All other
    errors will not be caught. Be prepared for KafkaUnavailableError and its
    ilk.

    This method force the client to use fresh metadata by calling
    kafka_client.load_metadata_for_topics(topics) before getting
    the group offsets.

    :param kafka_client: KafkaClient instance
    :param topic: topic name
    :param group: consumer group id
    :partitions: partitions list
    :returns: dict <partition>: <distance>
    """

    if partitions:
        topics = {topic: partitions}
    else:
        topics = [topic]
    consumer_offsets = get_consumer_offsets_metadata(
        kafka_client,
        group,
        topics,
    )
    return dict(
        [(offset.partition, offset.highmark - offset.current)
         for offset in consumer_offsets[topic]]
    )
