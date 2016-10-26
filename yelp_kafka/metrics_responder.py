from __future__ import absolute_import
from __future__ import unicode_literals

import abc
import logging


class MetricsResponder(object):
    """
        Used for publishing metrics with a metric responder instance
    """

    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)

    @abc.abstractmethod
    def get_counter_emitter(self, metric, default_dimensions=None):
        """
        Creates an instance for counting/counter a specific metric

        :param metric: the name of the metric
        :param default_dimensions: the extra dimensions provided for the metric
        :return: an instance of responder for recording counter based metrics
        """

        raise NotImplementedError

    @abc.abstractmethod
    def get_timer_emitter(self, metric, default_dimensions=None):
        """
        Creates and returns an instance for recording time elapsed
        for a specific metric

        :param metric: the name of the metric
        :param default_dimensions: the extra dimensions provided for the metric
        :return: an instance of responder for recording timer based metrics
        """

        raise NotImplementedError

    @abc.abstractmethod
    def record(self, registered_reporter, value, timestamp=None):
        """
        Used to record metrics for the registered reporter

        :param registered_reporter: The instance of the reporter
        :param value: The value to be recorded
        :param timestamp: The timestamp when the metric is recorded
        """

        raise NotImplementedError
