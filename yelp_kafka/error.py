class YelpKafkaError(Exception):
    pass


class ConsumerConfigurationError(YelpKafkaError):
    pass


class ConsumerGroupError(YelpKafkaError):
    pass
