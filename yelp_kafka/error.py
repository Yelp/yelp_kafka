class YelpKafkaError(Exception):
    """Base class for yelp_kafka errors."""
    pass


class DiscoveryError(YelpKafkaError):
    """Errors while using discovery functions."""
    pass


class ConsumerError(YelpKafkaError):
    """Error in consumer."""
    pass


class ConfigurationError(YelpKafkaError):
    """Error in configuration. For example. Missing configuration file
    or misformatted configuration."""
    pass


class ProcessMessageError(YelpKafkaError):
    """Error processing a message from kafka."""
    pass


class ConsumerGroupError(YelpKafkaError):
    """Error in the consumer group"""
    pass


class PartitionerError(YelpKafkaError):
    """Error in the partitioner"""
    pass


class UnknownTopic(YelpKafkaError):
    pass


class UnknownPartitions(YelpKafkaError):
    pass
