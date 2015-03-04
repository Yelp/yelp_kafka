from kafka.common import (
    OffsetFetchRequest,
    OffsetRequest,
    UnknownTopicOrPartitionError,
    check_error,
)

from yelp_kafka.error import (
    UnknownPartitions,
    UnknownTopic,
)


def offset_distance(kafka_client, topic, group, partitions=None):
    """Get the distance a group_id is from the current latest in a topic.

    If the group is unknown to kafka, it's assumed to be on offset 0. All other
    errors will not be caught. Be prepared for KafkaUnavailableError and its
    ilk.

    This method only does two calls over the network: one for the latest
    offsets for topic and current position of the group in that topic.
    Notabily, it does not query for metadata by itself. If might want to ensure
    the client has fresh metadata for the topic by calling
    `kafka_client.load_metadata_for_topics(topic)`.
    """

    if not kafka_client.has_metadata_for_topic(topic):
        raise UnknownTopic(
            "This kafka_client doesn't know about topic {0!r}. Consider"
            "calling `kafka_client.load_metadata_for_topics({0!r})`.".format(
                topic,
            ),
        )

    known_partitions = kafka_client.topic_partitions[topic]
    if not partitions:
        partitions = known_partitions
    else:
        unknown_partitions = set(partitions) - set(known_partitions)
        if unknown_partitions:
            raise UnknownPartitions(
                "This kafka_client doesn't know about partitions "
                "{partitions!r} for topic {topic!r}. Consider calling "
                "`kafka_client.load_metadata_for_topics({topic!r})`.".format(
                    topic=topic,
                    partitions=list(unknown_partitions),
                ),
            )

    latest_offsets = dict(
        (resp.partition, resp.offsets[0],)
        for resp in kafka_client.send_offset_request(
            [
                OffsetRequest(topic, partition, -1, 1)
                for partition in partitions
            ],
        )
    )

    def pluck_topic_offset_or_zero_on_unknown(resp):
        try:
            check_error(resp)
            return (resp.partition, resp.offset,)
        except UnknownTopicOrPartitionError:
            # If the server doesn't have any commited offsets by this group for
            # this topic, assume it's zero.
            return (resp.partition, 0,)

    group_offsets = dict(
        partition_offset
        for partition_offset in kafka_client.send_offset_fetch_request(
            group,
            [
                OffsetFetchRequest(topic, partition)
                for partition in partitions
            ],
            fail_on_error=False,
            callback=pluck_topic_offset_or_zero_on_unknown,
        )
    )

    return dict(
        (partition, latest_offsets[partition] - group_offsets[partition],)
        for partition in partitions
    )
