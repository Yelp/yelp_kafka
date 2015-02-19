import pytest
import mock

from kafka.common import KafkaUnavailableError

from yelp_kafka import utils


def test_make_scribe_topic():
    expected = 'scribe.datacenter.scribe_stream'
    assert expected == utils.make_scribe_topic(
        'scribe_stream', 'datacenter'
    )


@mock.patch("yelp_kafka.utils.KafkaClient", autospec=True)
def test_get_kafka_topics(mock_client):
    expected = {
        'topic1': [0, 1, 2, 3],
        'topic2': [0, 1]
    }
    mock_client.return_value.topic_partitions = expected
    actual = utils.get_kafka_topics(['mybroker'])
    mock_client.assert_called_once_with(['mybroker'])
    assert expected == actual


@mock.patch("yelp_kafka.utils.KafkaClient", autospec=True)
def test_get_kafka_topics_recover_from_error(mock_client):
    expected = {
        'topic1': [0, 1, 2, 3],
        'topic2': [0, 1]
    }
    mock_obj = mock_client.return_value
    mock_obj.topic_partitions = expected
    mock_obj.load_metadata_for_topics.side_effect = [KafkaUnavailableError(), None]
    actual = utils.get_kafka_topics(['mybroker'])
    mock_client.assert_called_once_with(['mybroker'])
    assert expected == actual


@mock.patch("yelp_kafka.utils.KafkaClient", autospec=True)
def test_get_kafka_topics_error(mock_client):
    expected = {
        'topic1': [0, 1, 2, 3],
        'topic2': [0, 1]
    }
    mock_obj = mock_client.return_value
    mock_obj.topic_partitions = expected
    mock_obj.load_metadata_for_topics.side_effect = KafkaUnavailableError('Boom!')
    with pytest.raises(KafkaUnavailableError):
        utils.get_kafka_topics(['mybroker'])
