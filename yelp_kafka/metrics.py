import logging
import math
import requests
import simplejson as json
import time
from multiprocessing import Event
from requests.exceptions import RequestException


SIGNALFX_ENDPOINT = "https://ingest.signalfx.com/v2/datapoint"
CONNECTION_TIMEOUT = 1  # Connection timeout in secs

TIME_METRIC_NAMES = [
    'metadata_request_timer',
    'produce_request_timer',
    'fetch_request_timer',
    'offset_request_timer',
    'offset_commit_request_timer',
    'offset_fetch_request_timer',
]

FAILURE_COUNT_METRIC_NAMES = [
    'failed_paylads_count',
    'out_of_range_counts',
    'not_leader_for_partition_count',
    'request_timed_out_count'
]


class MetricsReporter(object):
    def __init__(self, metric_prefix, queue, config):
        self.log = logging.getLogger(self.__class__.__name__)
        self.die_event = Event()

        self.metric_prefix = metric_prefix
        self.queue = queue
        self.group_id = config.group_id
        self.cluster_name = config.cluster.name
        self.extra_dimensions = config.signalfx_dimensions
        self.send_metrics_interval = config.signalfx_send_metrics_interval
        self.token = config.signalfx_token

    def main_loop(self):
        while not self.die_event.is_set():
            start_time = time.time()
            messages = []
            num_messages = self.queue.qsize()

            for _ in xrange(num_messages):
                messages.append(self.queue.get())

            self._process_metrics(messages)
            end_time = time.time()

            time_elapsed = end_time - start_time
            time_to_sleep = max(0, self.send_metrics_interval - time_elapsed)
            time.sleep(time_to_sleep)

    def _process_metrics(self, messages):
        time_metrics = dict((name, []) for name in TIME_METRIC_NAMES)
        failure_count_metrics = dict(
            (name, 0) for name in FAILURE_COUNT_METRIC_NAMES
        )

        for metric_name, datum in messages:
            if metric_name in time_metrics:
                time_metrics[metric_name].append(datum)
            elif metric_name in failure_count_metrics:
                failure_count_metrics[metric_name] += datum
            else:
                self.log.warn("Unknown metric: {0}".format(metric_name))

        gauges = []

        for metric, times in time_metrics.iteritems():
            metric_gauges = self._make_time_metric_data(metric, sorted(times))
            gauges.extend(metric_gauges)

        counters = []

        for metric, count in failure_count_metrics.iteritems():
            metric_counters = self._make_failure_count_data(metric, count)
            counters.extend(metric_counters)

        self._send_to_signalfx(gauges, counters)

    def _make_time_metric_data(self, metric, times):
        if not times:
            return []

        median = self._percentile(times, 0.5)
        per95 = self._percentile(times, 0.95)

        return [
            self._make_time_sfx_gauge(metric, median, 'median'),
            self._make_time_sfx_gauge(metric, per95, '95th')
        ]

    def _make_time_sfx_gauge(self, metric, value, type):
        return {
            'metric': self._make_signalfx_metric_name(metric),
            'value': value,
            'dimensions': self._make_dimensions({'type': type})
        }

    def _make_failure_count_data(self, metric, count):
        return [
            self._make_count_sfx_gauge(metric, count)
        ]

    def _make_count_sfx_gauge(self, metric, value):
        return {
            'metric': self._make_signalfx_metric_name(metric),
            'value': value,
            'dimensions': self._make_dimensions({})
        }

    def _make_signalfx_metric_name(self, metric):
        return self.metric_prefix + '.' + metric

    def _make_dimensions(self, data):
        dimensions = {
            'group_id': self.group_id,
            'cluster_name': self.cluster_name
        }

        dimensions.update(self.extra_dimensions)
        dimensions.update(data)
        return dimensions

    def _send_to_signalfx(self, gauges, counters):
        data = json.dumps({'gauge': gauges, 'counter': counters})
        headers = {'X-SF-Token': self.token}

        try:
            response = requests.post(SIGNALFX_ENDPOINT,
                                     data=data,
                                     headers=headers,
                                     timeout=CONNECTION_TIMEOUT)

            if not response.ok:
                msg = "Got bad response code from signalfx: {0}"
                self.log.warn(msg.format(response.status_code))
        except RequestException:
            self.log.exception("Sending data to signalfx failed")

    def _percentile(self, N, percent, key=lambda x: x):
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
