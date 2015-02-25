import mock
import pytest

from kafka.common import (
    OffsetResponse,
    OffsetFetchResponse,
)

from yelp_kafka.consumer import KafkaClient
from yelp_kafka.monitoring import (
    offset_distance,
    UnknownPartitions,
    UnknownTopic,
)


def kafka_client_mock():
    return mock.Mock(KafkaClient)


class TestOffsetDifference(object):
    topic = 'topic_name'
    group = 'group_name'
    topic_offsets = {
        0: 30,
        1: 30,
        2: 30,
    }
    group_offsets = {
        0: 30,
        1: 20,
        2: 10,
    }

    def kafka_client_mock(self):
        """A mock configured to support the happy path."""
        kafka_client_mock = mock.Mock(KafkaClient)

        kafka_client_mock.has_metadata_for_topic.side_effect = lambda t: \
            t == self.topic

        kafka_client_mock.topic_partitions = {
            self.topic: self.topic_offsets.keys(),
        }

        kafka_client_mock.send_offset_request.side_effect = \
            lambda reqs: [
                OffsetResponse(
                    self.topic,
                    req.partition,
                    0 if req.partition in self.topic_offsets else 3,
                    (self.topic_offsets.get(req.partition, -1),),
                )
                for req in reqs
            ]

        kafka_client_mock.send_offset_fetch_request.side_effect = \
            lambda group, reqs, fail_on_error, callback: [
                callback(
                    OffsetFetchResponse(
                        self.topic,
                        req.partition,
                        self.group_offsets.get(req.partition, -1),
                        None,
                        0 if req.partition in self.group_offsets else 3
                    ),
                )
                for req in reqs
            ]

        return kafka_client_mock

    def test_unknown_topic(self):
        with pytest.raises(UnknownTopic):
            offset_distance(
                self.kafka_client_mock(),
                topic="something that doesn't exist",
                group="this won't even be consulted",
            )

    def test_unknown_partitions(self):
        with pytest.raises(UnknownPartitions):
            offset_distance(
                self.kafka_client_mock(),
                topic=self.topic,
                group=self.group,
                partitions=[99],
            )

    def test_invalid_partition_subset(self):
        with pytest.raises(UnknownPartitions):
            offset_distance(
                self.kafka_client_mock(),
                topic=self.topic,
                group=self.group,
                partitions=[1, 99],
            )

    def test_ok(self):
        assert {0: 0, 1: 10, 2: 20} == offset_distance(
            self.kafka_client_mock(),
            topic=self.topic,
            group=self.group,
        )

    def test_partition_subset(self):
        assert {1: 10, 2: 20} == offset_distance(
            self.kafka_client_mock(),
            topic=self.topic,
            group=self.group,
            partitions=[1, 2],
        )

    def test_all_partitions(self):
        kafka_client = self.kafka_client_mock()

        implicit = offset_distance(
            kafka_client,
            topic=self.topic,
            group=self.group,
        )

        explicit = offset_distance(
            kafka_client,
            topic=self.topic,
            group=self.group,
            partitions=self.topic_offsets.keys(),
        )

        assert implicit == explicit

    def test_unknown_group(self):
        kafka_client_mock = self.kafka_client_mock()
        kafka_client_mock.send_offset_fetch_request.side_effect = \
            lambda group, reqs, fail_on_error, callback: [
                callback(
                    OffsetFetchResponse(req.topic, req.partition, -1, None, 3)
                )
                for req in reqs
            ]

        assert self.topic_offsets == offset_distance(
            kafka_client_mock,
            topic=self.topic,
            group='derp',
        )
