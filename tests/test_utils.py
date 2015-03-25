import pytest
import mock

from kafka.common import KafkaUnavailableError

from yelp_kafka import utils


def test_make_scribe_topic():
    expected = 'scribe.datacenter.scribe_stream'
    assert expected == utils.make_scribe_topic(
        'scribe_stream', 'datacenter'
    )


def test_get_kafka_topics():
    expected = {
        'topic1': [0, 1, 2, 3],
        'topic2': [0, 1]
    }
    mock_client = mock.Mock()
    mock_client.topic_partitions = expected
    actual = utils.get_kafka_topics(mock_client)
    assert expected == actual


def test_get_kafka_topics_recover_from_error():
    expected = {
        'topic1': [0, 1, 2, 3],
        'topic2': [0, 1]
    }
    mock_client = mock.Mock()
    mock_client.topic_partitions = expected
    mock_client.load_metadata_for_topics.side_effect = [KafkaUnavailableError(), None]
    actual = utils.get_kafka_topics(mock_client)
    assert expected == actual


def test_get_kafka_topics_error():
    expected = {
        'topic1': [0, 1, 2, 3],
        'topic2': [0, 1]
    }
    mock_client = mock.Mock()
    mock_client.topic_partitions = expected
    mock_client.load_metadata_for_topics.side_effect = KafkaUnavailableError('Boom!')
    with pytest.raises(KafkaUnavailableError):
        utils.get_kafka_topics(mock_client)
