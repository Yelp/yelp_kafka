import requests
import simplejson as json


SIGNALFX_ENDPOINT = "https://ingest.signalfx.com/v2/datapoint"
CONNECTION_TIMEOUT = 1  # Connection timeout in secs


def send_to_signalfx(token, gauges, counters):
    data = json.dumps({'gauge': gauges, 'counter': counters})
    headers = {'X-SF-Token': token}

    requests.post(SIGNALFX_ENDPOINT,
                  data=data,
                  headers=headers,
                  timeout=CONNECTION_TIMEOUT)
