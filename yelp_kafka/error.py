class YelpKafkaError(Exception):
    pass


class ConsumerConfigurationError(YelpKafkaError):
    pass
