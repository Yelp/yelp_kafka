import pytest

from yelp_kafka.config import load_config_or_default
from yelp_kafka.config import DEFAULT_CONFIG
from yelp_kafka.error import ConsumerConfigurationError


def test_missing_broker():
    config = {'group_id': 'test'}
    with pytest.raises(ConsumerConfigurationError):
        load_config_or_default(config)


def test_missing_consumer_group():
    config = {'brokers': 'test'}
    with pytest.raises(ConsumerConfigurationError):
        load_config_or_default(config)


def test_load_config():
    test_config = {
        'brokers': 'test_brokers',
        'group_id': 'test_group',
        'client_id': 'my-client'
    }
    config = load_config_or_default(test_config)
    assert config['brokers'] == 'test_brokers'
    assert config['group_id'] == 'test_group'
    assert config['client_id'] == 'my-client'
    assert all([k in config for k in DEFAULT_CONFIG.keys()])
