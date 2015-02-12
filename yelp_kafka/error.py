class YelpKafkaError(Exception):
    pass


class DiscoveryError(YelpKafkaError):
    pass


class ConsumerError(YelpKafkaError):
    pass


class ConfigurationError(YelpKafkaError):
    pass


class ConsumerConfigurationError(ConfigurationError):
    pass


class ProcessMessageError(YelpKafkaError):
    pass


class ConsumerGroupError(YelpKafkaError):
    pass


class PartitionerError(YelpKafkaError):
    pass
