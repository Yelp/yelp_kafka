from collections import defaultdict
from collections import namedtuple

from kafka.common import (
    OffsetFetchRequest,
    OffsetRequest,
    OffsetFetchResponse,
    UnknownTopicOrPartitionError,
    check_error,
)
from kafka.util import kafka_bytestring

from yelp_kafka.error import (
    UnknownPartitions,
    UnknownTopic,
)

ConsumerPartitionOffsets = namedtuple('ConsumerPartitionOffsets',
                                      ['partition', 'current',
                                       'highmark', 'lowmark'])


def pluck_topic_offset_or_zero_on_unknown(resp):
    try:
        check_error(resp)
    except UnknownTopicOrPartitionError:
        # If the server doesn't have any commited offsets by this group for
        # this topic, assume it's zero.
        pass
    # The API spec says server wont set an error, but 0.8.1.1 does. The actual
    # check is if the offset is -1.
    if resp.offset == -1:
        return OffsetFetchResponse(
            resp.topic, resp.partition, 0, resp.metadata, 0
        )
    return resp


def get_consumer_offsets_metadata(kafka_client, group, topics):
    """This method does four calls over the network:
        * refresh metadata for the kafka client
        * fetch group offsets
        * fetch earliest offsets
        * fetch latest offsets

    :param kafka_client: KafkaClient instance
    :param group: group id
    :topics: list of topics
    :returns: dict <topic>: [ConsumerPartitionOffsets]
    """

    if isinstance(topics, dict):
        topic_partitions = topics
    elif isinstance(topics, list):
        topic_partitions = dict([(topic, []) for topic in topics])
    else:
        raise TypeError("Invalid topics: {topics}. It must be either a "
                        "list of topics or a dict "
                        "topic: [partitions]".format(topics=topics))

    # Refresh client metadata. We do now use the topic list, because we
    # don't want to accidentally create the topic if it does not exist.
    kafka_client.load_metadata_for_topics()

    group_offset_reqs = []
    highmark_offset_reqs = []
    lowmark_offset_reqs = []
    for topic, partitions in topic_partitions.iteritems():
        if not kafka_client.has_metadata_for_topic(topic):
            raise UnknownTopic("Topic {topic!r} does not exist in "
                               "kafka".format(topic=topic))
        if partitions:
            # Check the partitions really exist
            unknown_partitions = set(partitions) - \
                set(kafka_client.get_partition_ids_for_topic(topic))
            if unknown_partitions:
                raise UnknownPartitions(
                    "Partitions {partitions!r} for topic {topic!r} do not"
                    "exist in kafka".format(partitions=partitions,
                                            topic=topic)
                )
        else:
            # Default get all partitions metadata
            partitions = kafka_client.get_partition_ids_for_topic(topic)
        for partition in partitions:
            # We perform 3 requests: group offsets, high watermark and low
            # watermark.
            group_offset_reqs.append(
                OffsetFetchRequest(kafka_bytestring(topic), partition)
            )
            highmark_offset_reqs.append(
                OffsetRequest(topic, partition, -1, max_offsets=1)
            )
            lowmark_offset_reqs.append(
                OffsetRequest(topic, partition, -2, max_offsets=1)
            )

    group_resps = kafka_client.send_offset_fetch_request(
        kafka_bytestring(group),
        group_offset_reqs,
        fail_on_error=False,
        callback=pluck_topic_offset_or_zero_on_unknown
    )
    highmark_resps = kafka_client.send_offset_request(highmark_offset_reqs)
    lowmark_resps = kafka_client.send_offset_request(lowmark_offset_reqs)

    # Sanity check. We should have responses for all topics and partitions,
    # so the two lists should have the same length.
    assert len(highmark_resps) == len(lowmark_resps)
    # Sanity check. The group offsets should be the same of
    # highmark/lowmark resps
    assert len(group_resps) <= len(highmark_resps)

    group_offsets = defaultdict(dict)
    for resp in group_resps:
        group_offsets[resp.topic][resp.partition] = resp.offset

    def aggregate_offsets(resps):
        offsets = defaultdict(dict)
        for resp in resps:
            offsets[resp.topic][resp.partition] = resp.offsets[0]
        return offsets

    highmark_offsets = aggregate_offsets(highmark_resps)
    lowmark_offsets = aggregate_offsets(lowmark_resps)
    result = {}
    for topic, partitions in highmark_offsets.iteritems():
        result[topic] = [
            ConsumerPartitionOffsets(
                partition=partition,
                current=group_offsets[topic][partition],
                highmark=highmark_offsets[topic][partition],
                lowmark=lowmark_offsets[topic][partition]
            ) for partition in partitions
        ]
    return result


def topics_offset_distance(kafka_client, topics, group):
    """Get the distance a group_id is from the current latest offset for topics.

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
        kafka_client, group, topics
    ).iteritems():
        distance[topic] = dict([(offset.partition, offset.highmark - offset.current)
                                for offset in offsets])
    return distance


def offset_distance(kafka_client, topic, group, partitions=None):
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
    consumer_offsets = get_consumer_offsets_metadata(kafka_client, group, topics)
    return dict(
        [(offset.partition, offset.highmark - offset.current)
         for offset in consumer_offsets[topic]]
    )
