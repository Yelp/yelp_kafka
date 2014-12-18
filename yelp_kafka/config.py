from kafka.consumer import AUTO_COMMIT_MSG_COUNT
from kafka.consumer import AUTO_COMMIT_INTERVAL
from kafka.consumer import FETCH_MIN_BYTES
from kafka.consumer import MAX_FETCH_BUFFER_SIZE_BYTES

from yelp_kafka.error import ConsumerConfigurationError

# This is fixed to 1 MB for making a fetch call more efficient when dealing
# with ranger messages can be more than 100KB in size
KAFKA_BUFFER_SIZE = 1024 * 1024

ZOOKEEPER_BASE_PATH = '/python-kakfa'
TIME_BOUNDARY = 30
MAX_WAITING_TIME = 10


DEFAULT_CONFIG = {
    'brokers': None,
    'client_id': 'yelp-kafka',
    'group_id': None,
    'buffer_size': KAFKA_BUFFER_SIZE,
    'auto_commit_every_n': AUTO_COMMIT_MSG_COUNT,
    'auto_commit_every_t': AUTO_COMMIT_INTERVAL,
    'auto_commit': True,
    'fetch_size_bytes': FETCH_MIN_BYTES,
    'max_buffer_size': MAX_FETCH_BUFFER_SIZE_BYTES,
    'iter_timeout': None,
    'zookeeper_base': ZOOKEEPER_BASE_PATH,
    'time_boundary': TIME_BOUNDARY,
    'max_waiting_time': MAX_WAITING_TIME,
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
    if 'group_id' not in config:
        raise ConsumerConfigurationError('group_id missing in config')
    if 'brokers' not in config:
        raise ConsumerConfigurationError('brokers missing in config')
    _config = {}
    for key in DEFAULT_CONFIG:
        _config[key] = config.pop(key, DEFAULT_CONFIG[key])
    return _config
