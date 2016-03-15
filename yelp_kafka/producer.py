# -*- coding: utf-8 -*-
import logging
import os
import socket

import yelp_meteorite
from kafka import SimpleProducer
from kafka.common import KafkaError

from yelp_kafka import metrics
from yelp_kafka.error import YelpKafkaError


METRIC_PREFIX = 'yelp_kafka.YelpKafkaProducer.'


class YelpKafkaSimpleProducer(SimpleProducer):
    """ YelpKafkaSimpleProducer is an extension of the kafka SimpleProducer that
    reports metrics about the producer to yelp_meteorite. These metrics include
    enqueue latency for both success and failure to send and the number of exceptions
    encountered trying to send.

    If metrics reporting isn't required for your producer, use kafka's SimpleProducer
    instead of this.

    Note: This producer expects usage of kafka-python==0.9.4.post2 where metrics_responder
    is implemented in KafkaClient

    Usage is the same as SimpleProducer:
        producer = YelpKafkaSimpleProducer(client)
        producer.send_messages("my_topic", "message1", "message2") # send msgs to kafka
    """

    def __init__(self, *args, **kwargs):
        super(YelpKafkaSimpleProducer, self).__init__(*args, **kwargs)
        self.log = logging.getLogger(self.__class__.__name__)
        self.client.metrics_responder = self._send_kafka_metrics
        self.timers = {}
        self.setup_metrics()

    def get_kafka_dimensions(self):
        return {'client_id': self.client.client_id}

    def get_default_dimensions(self):
        return {'hostname': self._get_hostname()}

    def setup_metrics(self):
        kafka_dimensions = self.get_kafka_dimensions()
        kafka_dimensions.update(self.get_default_dimensions())
        self.kafka_enqueue_exception_count = yelp_meteorite.create_counter(
            METRIC_PREFIX + metrics.PRODUCE_EXCEPTION_COUNT,
            kafka_dimensions
        )
        for name in metrics.TIME_METRIC_NAMES:
            self._create_timer(name, kafka_dimensions)

    def _send_kafka_metrics(self, key, value):
        if key in metrics.TIME_METRIC_NAMES:
            # kafka-python emits time in seconds, but yelp_meteorite wants
            # milliseconds
            time_in_ms = value * 1000
            self._get_timer(key).record(time_in_ms)
        else:
            self.log.warn("Unknown metric: {0}".format(key))

    def _create_timer(self, name, dimensions=None):
        if dimensions is None:
            dimensions = {}
        new_name = METRIC_PREFIX + name
        self.timers[new_name] = yelp_meteorite.create_timer(
            new_name,
            default_dimensions=dimensions
        )

    def _get_timer(self, name):
        return self.timers[METRIC_PREFIX + name]

    def send_messages(self, topic, *msg):
        try:
            super(YelpKafkaSimpleProducer, self).send_messages(topic, *msg)
        except (YelpKafkaError, KafkaError):
            self.kafka_enqueue_exception_count.count(1)
            raise

    def _get_hostname(self):
        hostname = socket.gethostname()
        # If we're in a docker container, gethostname returns the container id.
        # We can grab the actual hostname from the HOST env variable.
        HOST = os.getenv('HOST')
        return '{0}:{1}'.format(HOST, hostname) if HOST else hostname
