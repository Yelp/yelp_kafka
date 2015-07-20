import logging
import math
import requests
from requests.exceptions import RequestException
import simplejson as json
import time


SIGNALFX_ENDPOINT = "https://ingest.signalfx.com/v2/datapoint"
CONNECTION_TIMEOUT = 1  # Connection timeout in secs


class MetricsReporter(object):
    def __init__(self, queue, config):
        self.log = logging.getLogger(self.__class__.__name__)

        self.queue = queue
        self.group_id = config.group_id
        self.cluster_name = config.cluster.name
        self.extra_dimensions = config.signalfx_dimensions
        self.send_metrics_interval = config.signalfx_send_metrics_interval
        self.token = config.signalfx_token

    def main_loop(self):
        while True:
            start_time = time.time()
            messages = []
            num_messages = self.queue.qsize()

            for _ in xrange(num_messages):
                messages.append(self.queue.get())

            self.process_metrics(messages)
            end_time = time.time()

            time_elapsed = end_time - start_time
            time_to_sleep = max(0, self.send_metrics_interval - time_elapsed)
            time.sleep(time_to_sleep)

    def process_metrics(self, messages):
        time_metrics = {
            'metadata_request_timer': [],
            'produce_request_timer': [],
            'fetch_request_timer': [],
            'offset_request_timer': [],
            'offset_commit_request_timer': [],
            'offset_fetch_request_timer': []
        }

        failure_count_metrics = {
            'failed_paylads_count': 0,
            'out_of_range_counts': 0,
            'not_leader_for_partition_count': 0,
            'request_timed_out_count': 0
        }

        for metric_name, datum in messages:
            if metric_name in time_metrics:
                time_metrics[metric_name].append(datum)
            elif metric_name in failure_count_metrics:
                failure_count_metrics[metric_name] += datum
            else:
                raise Exception("Unknown metric: {0}".format(metric_name))

        gauges = []

        for metric, times in time_metrics.iteritems():
            metric_gauges = self.make_time_metric_data(metric, sorted(times))
            gauges.extend(metric_gauges)

        counters = []

        for metric, count in failure_count_metrics.iteritems():
            metric_counters = self.make_failure_count_data(metric, count)
            counters.extend(metric_counters)

        self.send_to_signalfx(gauges, counters)

    def make_time_metric_data(self, metric, times):
        if not times:
            return []

        median = self.percentile(times, 0.5)
        per95 = self.percentile(times, 0.95)

        return [
            self.make_time_sfx_gauge(metric, median, 'median'),
            self.make_time_sfx_gauge(metric, per95, '95th')
        ]

    def make_time_sfx_gauge(self, metric, value, type):
        return {
            'metric': self.make_signalfx_metric_name(metric),
            'value': value,
            'dimensions': self.make_dimensions({'type': type})
        }

    def make_failure_count_data(self, metric, count):
        return [
            self.make_count_sfx_gauge(metric, count)
        ]

    def make_count_sfx_gauge(self, metric, value):
        return {
            'metric': self.make_signalfx_metric_name(metric),
            'value': value,
            'dimensions': self.make_dimensions({})
        }

    def make_signalfx_metric_name(self, metric):
        return 'yelp_kafka.KafkaConsumerGroup.' + metric

    def make_dimensions(self, data):
        dimensions = {
            'group_id': self.group_id,
            'cluster_name': self.cluster_name
        }

        dimensions.update(self.extra_dimensions)
        dimensions.update(data)
        return dimensions

    def send_to_signalfx(self, gauges, counters):
        data = json.dumps({'gauge': gauges, 'counter': counters})
        headers = {'X-SF-Token': self.token}

        try:
            requests.post(SIGNALFX_ENDPOINT,
                          data=data,
                          headers=headers,
                          timeout=CONNECTION_TIMEOUT)
        except RequestException:
            self.log.exception("Sending data to signalfx failed")

    def percentile(self, N, percent, key=lambda x: x):
        """
        Find the percentile of a list of values.

        @parameter N - is a list of values. Note N MUST BE already sorted.
        @parameter percent - a float value from 0.0 to 1.0.
        @parameter key - optional key function to compute value from each element of N.

        @return - the percentile of the values
        """
        if not N:
            return None
        k = (len(N) - 1) * percent
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return key(N[int(k)])
        d0 = key(N[int(f)]) * (c - k)
        d1 = key(N[int(c)]) * (k - f)
        return d0 + d1
