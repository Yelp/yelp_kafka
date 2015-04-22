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


def test_split_scribe_topic():
    topic = "scribe.uswest1-devc.ranger"
    datacentre, stream = utils.split_scribe_topic(topic)
    assert datacentre == "uswest1-devc"
    assert stream == "ranger"

    topic = "scribeuswest1-devcranger"
    with pytest.raises(ValueError):
        utils.split_scribe_topic(topic)

    topic = "scribe.uswest.ranger.garbage"
    with pytest.raises(ValueError):
        utils.split_scribe_topic(topic)
