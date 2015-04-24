import mock
import pytest

from kafka.common import (
    OffsetResponse,
    OffsetFetchResponse,
    KafkaUnavailableError,
)

from yelp_kafka.consumer import KafkaClient
from yelp_kafka.monitoring import (
    ConsumerPartitionOffsets,
    get_consumer_offsets_metadata,
    get_topics_watermarks,
    get_current_consumer_offsets,
    offset_distance,
    PartitionOffsets,
    topics_offset_distance,
    UnknownPartitions,
    UnknownTopic,
)


def kafka_client_mock():
    return mock.Mock(KafkaClient)


@pytest.fixture(params=[['topic1'], set(['topic1']), ('topic1',)])
def topics(request):
    return request.param


class TestOffsetDifference(object):
    topics = {
        'topic1': [0, 1, 2],
        'topic2': [0, 1]
    }
    group = 'group_name'
    high_offsets = {
        'topic1': {
            0: 30,
            1: 30,
            2: 30,
        },
        'topic2': {
            0: 50,
            1: 50
        }
    }
    low_offsets = {
        'topic1': {
            0: 10,
            1: 5,
            2: 3,
        },
        'topic2': {
            0: 5,
            1: 5
        }
    }
    group_offsets = {
        'topic1': {
            0: 30,
            1: 20,
            2: 10,
        },
        'topic2': {
            0: 15
        }
    }

    def kafka_client_mock(self):
        """A mock configured to support the happy path."""
        kafka_client_mock = mock.Mock(KafkaClient)

        kafka_client_mock.has_metadata_for_topic.side_effect = lambda t: \
            t in self.topics

        kafka_client_mock.topic_partitions = self.topics

        def send_offset_request(payloads=[], fail_on_error=True, callback=None):
            resps = []
            for req in payloads:
                if req.time == -1:
                    offset = self.high_offsets[req.topic].get(req.partition, -1)
                else:
                    offset = self.low_offsets[req.topic].get(req.partition, -1)
                resps.append(OffsetResponse(
                    req.topic,
                    req.partition,
                    0 if req.partition in self.topics[req.topic] else 3,
                    (offset,)
                ))
            return resps

        kafka_client_mock.send_offset_request.side_effect = \
            send_offset_request

        kafka_client_mock.get_partition_ids_for_topic.side_effect = \
            lambda topic: self.topics[topic]

        kafka_client_mock.send_offset_fetch_request.side_effect = \
            lambda group, reqs, fail_on_error, callback: [
                callback(
                    OffsetFetchResponse(
                        req.topic,
                        req.partition,
                        self.group_offsets[req.topic].get(req.partition, -1),
                        None,
                        0 if req.partition in self.group_offsets[req.topic] else 3
                    ),
                )
                for req in reqs
            ]

        return kafka_client_mock

    def test_get_current_consumer_offsets_invalid_arguments(self):
        with pytest.raises(TypeError):
            get_current_consumer_offsets(
                self.kafka_client_mock(),
                "this won't even be consulted",
                "this should be a list or dict",
            )

    def test_get_current_consumer_offsets_unknown_topic(self):
        with pytest.raises(UnknownTopic):
            get_current_consumer_offsets(
                self.kafka_client_mock(),
                "this won't even be consulted",
                ["something that doesn't exist"],
            )

    def test_get_current_consumer_offsets_unknown_topic_no_fail(self):
        actual = get_current_consumer_offsets(
            self.kafka_client_mock(),
            "this won't even be consulted",
            ["something that doesn't exist"],
            fail_on_error=False
        )
        assert not actual

    def test_get_current_consumer_offsets_unknown_partitions(self):
        with pytest.raises(UnknownPartitions):
            get_current_consumer_offsets(
                self.kafka_client_mock(),
                self.group,
                {'topic1': [99]},
            )

    def test_get_current_consumer_offsets_unknown_partitions_no_fail(self):
        actual = get_current_consumer_offsets(
            self.kafka_client_mock(),
            self.group,
            {'topic1': [99]},
            fail_on_error=False
        )
        assert not actual

    def test_get_current_consumer_offsets_invalid_partition_subset(self):
        with pytest.raises(UnknownPartitions):
            get_current_consumer_offsets(
                self.kafka_client_mock(),
                self.group,
                {'topic1': [1, 99]},
            )

    def test_get_current_consumer_offsets_invalid_partition_subset_no_fail(self):
        actual = get_current_consumer_offsets(
            self.kafka_client_mock(),
            self.group,
            {'topic1': [1, 99]},
            fail_on_error=False
        )
        assert actual['topic1'][1] == 20
        # Partition 99 does not exist so it shouldn't be in the result
        assert 99 not in actual['topic1']

    def test_get_current_consumer_offsets(self, topics):
        actual = get_current_consumer_offsets(
            self.kafka_client_mock(),
            self.group,
            topics
        )
        assert actual == {'topic1': {0: 30, 1: 20, 2: 10}}

    def test_get_topics_watermarks_invalid_arguments(self):
        with pytest.raises(TypeError):
            get_topics_watermarks(
                self.kafka_client_mock(),
                "this should be a list or dict",
            )

    def test_get_topics_watermarks_unknown_topic(self):
        with pytest.raises(UnknownTopic):
            get_topics_watermarks(
                self.kafka_client_mock(),
                ["something that doesn't exist"],
            )

    def test_get_topics_watermarks_unknown_topic_no_fail(self):
        actual = get_topics_watermarks(
            self.kafka_client_mock(),
            ["something that doesn't exist"],
            fail_on_error=False,
        )
        assert not actual

    def test_get_topics_watermarks_unknown_partitions(self):
        with pytest.raises(UnknownPartitions):
            get_topics_watermarks(
                self.kafka_client_mock(),
                {'topic1': [99]},
            )

    def test_get_topics_watermarks_unknown_partitions_no_fail(self):
        actual = get_topics_watermarks(
            self.kafka_client_mock(),
            {'topic1': [99]},
            fail_on_error=False,
        )
        assert not actual

    def test_get_topics_watermarks_invalid_partition_subset(self):
        with pytest.raises(UnknownPartitions):
            get_topics_watermarks(
                self.kafka_client_mock(),
                {'topic1': [1, 99]},
            )

    def test_get_topics_watermarks_invalid_partition_subset_no_fail(self):
        actual = get_topics_watermarks(
            self.kafka_client_mock(),
            {'topic1': [1, 99]},
            fail_on_error=False,
        )
        assert actual['topic1'][1] == PartitionOffsets('topic1', 1, 30, 5)
        assert 99 not in actual['topic1']

    def test_get_topics_watermarks(self, topics):
        actual = get_topics_watermarks(
            self.kafka_client_mock(),
            topics,
        )
        assert actual == {'topic1': {
            0: PartitionOffsets('topic1', 0, 30, 10),
            1: PartitionOffsets('topic1', 1, 30, 5),
            2: PartitionOffsets('topic1', 2, 30, 3),
        }}

    def test_offset_metadata_invalid_arguments(self):
        with pytest.raises(TypeError):
            get_consumer_offsets_metadata(
                self.kafka_client_mock(),
                "this won't even be consulted",
                "this should be a list or dict",
            )

    def test_offset_metadata_unknown_topic(self):
        with pytest.raises(UnknownTopic):
            get_consumer_offsets_metadata(
                self.kafka_client_mock(),
                "this won't even be consulted",
                ["something that doesn't exist"],
            )

    def test_offset_metadata_unknown_topic_no_fail(self):
        actual = get_consumer_offsets_metadata(
            self.kafka_client_mock(),
            "this won't even be consulted",
            ["something that doesn't exist"],
            fail_on_error=False
        )
        assert not actual

    def test_offset_metadata_unknown_partitions(self):
        with pytest.raises(UnknownPartitions):
            get_consumer_offsets_metadata(
                self.kafka_client_mock(),
                self.group,
                {'topic1': [99]},
            )

    def test_offset_metadata_unknown_partitions_no_fail(self):
        actual = get_consumer_offsets_metadata(
            self.kafka_client_mock(),
            self.group,
            {'topic1': [99]},
            fail_on_error=False
        )
        assert not actual

    def test_offset_metadata_invalid_partition_subset(self):
        with pytest.raises(UnknownPartitions):
            get_consumer_offsets_metadata(
                self.kafka_client_mock(),
                self.group,
                {'topic1': [1, 99]},
            )

    def test_offset_metadata_invalid_partition_subset_no_fail(self):
        # Partition 99 does not exist, so we expect to have
        # offset metadata ONLY for partition 1.
        expected = [
            ConsumerPartitionOffsets('topic1', 1, 20, 30, 5)
        ]

        actual = get_consumer_offsets_metadata(
            self.kafka_client_mock(),
            self.group,
            {'topic1': [1, 99]},
            fail_on_error=False
        )
        assert 'topic1' in actual
        assert actual['topic1'] == expected

    def test_get_metadata_kafka_error(self):
        kafka_client_mock = self.kafka_client_mock()
        kafka_client_mock.load_metadata_for_topics.side_effect = KafkaUnavailableError("Boom!")
        with pytest.raises(KafkaUnavailableError):
            get_consumer_offsets_metadata(kafka_client_mock,
                                          self.group,
                                          {'topic1': [99]},)
        assert kafka_client_mock.load_metadata_for_topics.call_count == 2

    def test_offset_distance_ok(self):
        assert {0: 0, 1: 10, 2: 20} == offset_distance(
            self.kafka_client_mock(),
            self.group,
            'topic1',
        )

    def test_offset_distance_partition_subset(self):
        assert {1: 10, 2: 20} == offset_distance(
            self.kafka_client_mock(),
            self.group,
            'topic1',
            partitions=[1, 2],
        )

    def test_offset_distance_all_partitions(self):
        kafka_client = self.kafka_client_mock()

        implicit = offset_distance(
            kafka_client,
            self.group,
            'topic1',
        )

        explicit = offset_distance(
            kafka_client,
            self.group,
            'topic1',
            partitions=self.high_offsets['topic1'].keys(),
        )

        assert implicit == explicit

    def test_offset_distance_unknown_group(self):
        kafka_client_mock = self.kafka_client_mock()
        kafka_client_mock.send_offset_fetch_request.side_effect = \
            lambda group, reqs, fail_on_error, callback: [
                callback(
                    OffsetFetchResponse(req.topic, req.partition, -1, None, 3)
                )
                for req in reqs
            ]

        assert self.high_offsets['topic1'] == offset_distance(
            kafka_client_mock,
            'derp',
            'topic1',
        )

    def test_topics_offset_distance(self):
        expected = {
            'topic1': {0: 0, 1: 10, 2: 20},
            'topic2': {0: 35, 1: 50}
        }
        assert expected == topics_offset_distance(
            self.kafka_client_mock(),
            self.group,
            ['topic1', 'topic2'],
        )

    def test_topics_offset_distance_partition_subset(self):
        expected = {
            'topic1': {0: 0, 1: 10},
            'topic2': {1: 50}
        }
        assert expected == topics_offset_distance(
            self.kafka_client_mock(),
            self.group,
            {'topic1': [0, 1], 'topic2': [1]},
        )

    def test_topics_offset_distance_topic_subset(self):
        expected = {
            'topic1': {0: 0, 1: 10},
        }
        assert expected == topics_offset_distance(
            self.kafka_client_mock(),
            self.group,
            {'topic1': [0, 1]},
        )
