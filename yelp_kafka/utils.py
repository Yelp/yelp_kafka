# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import functools
import logging

from kafka.common import KafkaUnavailableError
from six.moves import cPickle as pickle


log = logging.getLogger(__name__)


def get_kafka_topics(kafkaclient):
    """Connect to kafka and fetch all the topics/partitions."""
    try:
        kafkaclient.load_metadata_for_topics()
    except KafkaUnavailableError:
        # Sometimes the kafka server closes the connection for inactivity
        # in this case the second call should succeed otherwise the kafka
        # server is down and we should fail
        log.debug("First call to kafka for loading metadata failed."
                  " Trying again.")
        kafkaclient.load_metadata_for_topics()
    return kafkaclient.topic_partitions


def make_scribe_topic(stream, datacenter):
    """Get a scribe topic name

    :param stream: scribe stream name
    :param datacenter: datacenter name
    :returns: topic name
    """
    return "scribe.{0}.{1}".format(datacenter, stream)


def _split_topic_name(topic_name):
    tokens = topic_name.split(".", 2)
    if len(tokens) < 3 or tokens[0] != "scribe":
        raise ValueError("Encountered wrongly formatted topic %s" % topic_name)
    else:
        return tokens


def extract_datacenter(topic_name):
    """Get the datacenter from a kafka topic name

    :param topic_name: Kafka topic name
    :returns: datacenter
    :raises: ValueError if the topic name does not conform to the expected
             format: "scribe.<datacenter>.<stream name>"
    """
    return _split_topic_name(topic_name)[1]


def extract_stream_name(topic_name):
    """Get the stream name from a kafka topic name

    :param topic_name: Kafka topic name
    :returns: stream name
    :raises: ValueError if the topic name does not conform to the expected
               format: "scribe.<datacenter>.<stream name>"
    """
    return _split_topic_name(topic_name)[2]


def retry_if_kafka_unavailable_error(exception):
    return isinstance(exception, KafkaUnavailableError)


class memoized(object):
    """Decorator that caches a function's return value each time it is called.
    If called later with the same arguments, the cached value is returned, and
    the function is not re-evaluated.

    Based upon from http://wiki.python.org/moin/PythonDecoratorLibrary#Memoize
    Nota bene: this decorator memoizes /all/ calls to the function.  For a memoization
    decorator with limited cache size, consider:
    http://code.activestate.com/recipes/496879-memoize-decorator-function-with-cache-size-limit/
    """

    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, *args, **kwargs):
        # If the function args cannot be used as a cache hash key, fail fast
        key = pickle.dumps((args, kwargs))
        try:
            return self.cache[key]
        except KeyError:
            value = self.func(*args, **kwargs)
            self.cache[key] = value
            return value

    def __repr__(self):
        """Return the function's docstring."""
        return self.func.__doc__

    def __get__(self, obj, objtype):
        """Support instance methods."""
        return functools.partial(self.__call__, obj)
