from kafka.consumer import AUTO_COMMIT_MSG_COUNT
from kafka.consumer import AUTO_COMMIT_INTERVAL
from kafka.consumer import FETCH_MIN_BYTES

from yelp_kafka.error import ConsumerConfigurationError

# This is fixed to 1 MB for making a fetch call more efficient when dealing
# with ranger messages can be more than 100KB in size
KAFKA_BUFFER_SIZE = 1024 * 1024  # 1MB

ZOOKEEPER_BASE_PATH = '/python-kafka'
ZK_PARTITIONER_COOLDOWN = 30
MAX_TERMINATION_TIMEOUT_SECS = 10
MAX_ITERATOR_TIMEOUT_SECS = 0.1

""" yelp_kafka configuration default values:
    .. code:: python
        brokers=None,  # Must be specified in configuration
        group_id=None,  # Must be specified in configuration
        client_id='yelp-kafka',
        buffer_size=1024 * 1024,  # 1MB
        auto_commit_every_n=AUTO_COMMIT_MSG_COUNT,  # See kafka-python
        auto_commit_every_t=AUTO_COMMIT_INTERVAL,  # See kafka-python
        fetch_size_bytes=FETCH_MIN_BYTES,  # See kafka-python
        max_buffer_size=None,  # No limit
        iter_timeout=0.1,
        zookeeper_base= '/python-kafka',
        zk_partitioner_cooldown= 30,
        max_termination_timeout_secs=10,
        latest_offset=True
"""

DEFAULT_CONFIG = {
    'brokers': None,
    'client_id': 'yelp-kafka',
    'group_id': None,
    'buffer_size': KAFKA_BUFFER_SIZE,
    'auto_commit_every_n': AUTO_COMMIT_MSG_COUNT,
    'auto_commit_every_t': AUTO_COMMIT_INTERVAL,
    'auto_commit': True,
    'fetch_size_bytes': FETCH_MIN_BYTES,
    'max_buffer_size': None,
    'iter_timeout': MAX_ITERATOR_TIMEOUT_SECS,
    'zookeeper_base': ZOOKEEPER_BASE_PATH,
    'zk_partitioner_cooldown': ZK_PARTITIONER_COOLDOWN,
    'max_termination_timeout_secs': MAX_TERMINATION_TIMEOUT_SECS,
    'latest_offset': True
}


CONSUMER_CONFIG_KEYS = (
    'buffer_size',
    'auto_commit_every_n',
    'auto_commit_every_t',
    'auto_commit',
    'fetch_size_bytes',
    'max_buffer_size',
    'iter_timeout'
)


def load_config_or_default(config):
    """Load configuration from dict.
    This function is used from both KafkaSimpleConsumer and ConsumerGroup.

    :param config: user configuration dict
    :returns: a valid configuration for either ConsuerGroup or KafkaSimpleConsumer
    :rtype: dict
    :raises ConsumerConfigurationError: if either group_id or brokers are not specified
    """
    if 'group_id' not in config:
        raise ConsumerConfigurationError('group_id missing in config')
    if 'brokers' not in config:
        raise ConsumerConfigurationError('brokers missing in config')
    _config = {}
    for key in DEFAULT_CONFIG:
        _config[key] = config.get(key, DEFAULT_CONFIG[key])
    return _config
