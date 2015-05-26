import mock
import pytest
import copy

from kafka.common import (
    OffsetResponse,
    OffsetFetchResponse,
    OffsetCommitResponse,
    RequestTimedOutError,
    KafkaUnavailableError,
)

from yelp_kafka.consumer import KafkaClient
from yelp_kafka.monitoring import (
    ConsumerPartitionOffsets,
    get_topics_watermarks,
    get_current_consumer_offsets,
    PartitionOffsets,
    UnknownPartitions,
    UnknownTopic,
    OffsetCommitError,
    _verify_commit_offsets_requests,
    advance_consumer_offsets,
    rewind_consumer_offsets,
    set_consumer_offsets,
    get_consumer_offsets_metadata,
    offset_distance,
    topics_offset_distance,
)


def kafka_client_mock():
    return mock.Mock(KafkaClient)


@pytest.fixture(params=[['topic1'], set(['topic1']), ('topic1',)])
def topics(request):
    return request.param


class TestOffsetsBase(object):
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
            1: 5,
        }
    }
    group_offsets = {
        'topic1': {
            0: 30,
            1: 20,
            2: 10,
        },
        'topic2': {
            0: 15,
        }
    }

    def reset_group_offsets(self):
        self.group_offsets['topic1'][0] = 30
        self.group_offsets['topic1'][1] = 20
        self.group_offsets['topic1'][2] = 10
        self.group_offsets['topic2'][0] = 15
        del self.group_offsets['topic2'][1]

    def kafka_client_mock(self, commit_error=False):
        """A mock configured to support the happy path."""
        kafka_client_mock = mock.Mock(KafkaClient)

        kafka_client_mock.has_metadata_for_topic.side_effect = lambda t: \
            t in self.topics

        kafka_client_mock.topic_partitions = self.topics

        def send_offset_request(payloads=None, fail_on_error=True, callback=None):
            if payloads is None:
                payloads = []

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

        def send_offset_commit_request(
            group, payloads=None,
            fail_on_error=True, callback=None
        ):
            if payloads is None:
                payloads = []

            resps = []
            for req in payloads:
                if not commit_error:
                    self.group_offsets[req.topic][req.partition] = req.offset
                    resps.append(
                        OffsetCommitResponse(
                            req.topic,
                            req.partition,
                            0
                        )
                    )
                else:
                    resps.append(
                        OffsetCommitResponse(
                            req.topic,
                            req.partition,
                            RequestTimedOutError.errno
                        )
                    )

            return [resp if not callback else callback(resp) for resp in resps]

        kafka_client_mock.send_offset_commit_request.side_effect = \
            send_offset_commit_request

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


class TestMonitoring(TestOffsetsBase):
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
            raise_on_error=False
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
            raise_on_error=False
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
            raise_on_error=False
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


class TestOffsets(TestOffsetsBase):
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
            raise_on_error=False
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
            raise_on_error=False
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
            raise_on_error=False
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
            raise_on_error=False,
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
            raise_on_error=False,
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
            raise_on_error=False,
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

    def test__verify_commit_offsets_requests(self):
        new_offsets = {
            'topic1': {
                0: 123,
                1: 456,
            },
            'topic2': {
                0: 12,
            },
        }
        valid_new_offsets = _verify_commit_offsets_requests(
            self.kafka_client_mock(),
            new_offsets,
            True
        )

        assert new_offsets == valid_new_offsets

    def test__verify_commit_offsets_requests_invalid_types(self):
        new_offsets = "my_str"
        with pytest.raises(TypeError):
            _verify_commit_offsets_requests(
                self.kafka_client_mock(),
                new_offsets,
                True
            )

        new_offsets = {'topic1': 2, 'topic2': 1}
        with pytest.raises(TypeError):
            _verify_commit_offsets_requests(
                self.kafka_client_mock(),
                new_offsets,
                False
            )

        new_offsets = {
            'topic1': [
                PartitionOffsets('topic1', 0, 1, 2),
                PartitionOffsets('topic2', 0, 1, 2),
            ],
            'topic2': [
                ConsumerPartitionOffsets('topic1', 0, 1, 2, 3),
            ],
        }
        with pytest.raises(TypeError):
            _verify_commit_offsets_requests(
                self.kafka_client_mock(),
                new_offsets,
                True
            )

    def test__verify_commit_offsets_requests_bad_metadata(self):
        new_offsets = {
            'topic1': {
                23: 123,
                11: 456,
            },
            'topic2': {
                21: 12,
            },
        }
        with pytest.raises(UnknownPartitions):
            _verify_commit_offsets_requests(
                self.kafka_client_mock(),
                new_offsets,
                True
            )

        new_offsets = {
            'topic32': {
                0: 123,
                1: 456,
            },
            'topic33': {
                0: 12,
            },
        }
        with pytest.raises(UnknownTopic):
            _verify_commit_offsets_requests(
                self.kafka_client_mock(),
                new_offsets,
                True
            )

    def test__verify_commit_offsets_requests_bad_metadata_no_fail(self):
        new_offsets = {
            'topic1': {
                0: 32,
                23: 123,
                11: 456,
            },
            'topic2': {
                21: 12,
            },
        }
        valid_new_offsets = _verify_commit_offsets_requests(
            self.kafka_client_mock(),
            new_offsets,
            False
        )
        expected_valid_offsets = {
            'topic1': {
                0: 32,
            },
        }

        assert valid_new_offsets == expected_valid_offsets

        new_offsets = {
            'topic32': {
                0: 123,
                1: 456,
            },
            'topic33': {
                0: 12,
            },
        }
        valid_new_offsets = _verify_commit_offsets_requests(
            self.kafka_client_mock(),
            new_offsets,
            False
        )
        assert valid_new_offsets == {}

    def test_advance_consumer_offsets(self):
        topics = {
            'topic1': [0, 1, 2],
            'topic2': [0, 1],
        }
        status = advance_consumer_offsets(
            self.kafka_client_mock(),
            "group",
            topics
        )
        assert status == []
        assert self.group_offsets == self.high_offsets
        self.reset_group_offsets()

    def test_advance_consumer_offsets_fail(self):
        group_offsets_old = copy.deepcopy(self.group_offsets)
        topics = {
            'topic1': [0, 1, 2],
            'topic2': [0, 1],
        }
        expected_status = [
            OffsetCommitError("topic1", 0, RequestTimedOutError.message),
            OffsetCommitError("topic1", 1, RequestTimedOutError.message),
            OffsetCommitError("topic1", 2, RequestTimedOutError.message),
            OffsetCommitError("topic2", 0, RequestTimedOutError.message),
            OffsetCommitError("topic2", 1, RequestTimedOutError.message),
        ]

        status = advance_consumer_offsets(
            self.kafka_client_mock(commit_error=True),
            "group",
            topics
        )
        assert status == expected_status
        assert self.group_offsets == group_offsets_old

        status = advance_consumer_offsets(
            self.kafka_client_mock(commit_error=True),
            "group",
            topics,
            raise_on_error=False
        )
        assert status == expected_status
        assert self.group_offsets == group_offsets_old

    def test_rewind_consumer_offsets(self):
        topics = {
            'topic1': [0, 1, 2],
            'topic2': [0, 1],
        }
        status = rewind_consumer_offsets(
            self.kafka_client_mock(),
            "group",
            topics
        )

        status == []
        assert self.group_offsets == self.low_offsets
        self.reset_group_offsets()

    def test_rewind_consumer_offsets_fail(self):
        group_offsets_old = copy.deepcopy(self.group_offsets)
        topics = {
            'topic1': [0, 1, 2],
            'topic2': [0, 1],
        }
        expected_status = [
            OffsetCommitError("topic1", 0, RequestTimedOutError.message),
            OffsetCommitError("topic1", 1, RequestTimedOutError.message),
            OffsetCommitError("topic1", 2, RequestTimedOutError.message),
            OffsetCommitError("topic2", 0, RequestTimedOutError.message),
            OffsetCommitError("topic2", 1, RequestTimedOutError.message),
        ]

        status = rewind_consumer_offsets(
            self.kafka_client_mock(commit_error=True),
            "group",
            topics
        )
        assert status == expected_status
        assert self.group_offsets == group_offsets_old

        status = rewind_consumer_offsets(
            self.kafka_client_mock(commit_error=True),
            "group",
            topics,
            raise_on_error=False
        )
        assert status == expected_status
        assert self.group_offsets == group_offsets_old

    def test_set_consumer_offsets(self):
        new_offsets = {
            'topic1': {
                0: 100,
                1: 200,
            },
            'topic2': {
                0: 150,
                1: 300,
            },
        }

        status = set_consumer_offsets(
            self.kafka_client_mock(),
            "group",
            new_offsets
        )

        expected_offsets = {
            'topic1': {
                0: 100,
                1: 200,
                2: 10,
            },
            'topic2': {
                0: 150,
                1: 300,
            }
        }
        assert status == []
        assert self.group_offsets == expected_offsets
        self.reset_group_offsets()

    def test_set_consumer_offsets_fail(self):
        group_offsets_old = copy.deepcopy(self.group_offsets)
        new_offsets = {
            'topic1': {
                0: 100,
                1: 200,
            },
            'topic2': {
                0: 150,
                1: 300,
            },
        }
        expected_status = [
            OffsetCommitError("topic1", 0, RequestTimedOutError.message),
            OffsetCommitError("topic1", 1, RequestTimedOutError.message),
            OffsetCommitError("topic2", 0, RequestTimedOutError.message),
            OffsetCommitError("topic2", 1, RequestTimedOutError.message),
        ]

        status = set_consumer_offsets(
            self.kafka_client_mock(commit_error=True),
            "group",
            new_offsets,
            raise_on_error=True
        )

        assert status == expected_status
        assert self.group_offsets == group_offsets_old

        status = set_consumer_offsets(
            self.kafka_client_mock(commit_error=True),
            "group",
            new_offsets,
            raise_on_error=False
        )
        assert status == expected_status
        assert self.group_offsets == group_offsets_old
