class YelpKafkaError(Exception):
    pass


class ConsumerError(YelpKafkaError):
    pass


class ConsumerConfigurationError(ConsumerError):
    pass


class ProcessMessageError(YelpKafkaError):
    pass


class ConsumerGroupError(YelpKafkaError):
    pass


class PartitionerError(YelpKafkaError):
    pass


class UnknownTopic(YelpKafkaError):
    pass


class UnknownPartitions(YelpKafkaError):
    pass
