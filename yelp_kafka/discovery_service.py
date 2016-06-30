import logging
import requests
from yelp_kafka.config import ClusterConfig
from yelp_kafka.error import ConfigurationError

REGION_FILE_PATH = '/nail/etc/region'
# TODO: base query?
# MOVE to config?
BASE_QUERY = 'http://yocalhost:20495/v1'


log = logging.getLogger(__name__)


def get_local_region():
    """Get local-region name."""
    try:
        with open(REGION_FILE_PATH, 'r') as region_file:
            return region_file.read().rstrip()
    except IOError:
        err_msg = "Could not region information at {file}".format(file=REGION_FILE_PATH)
        log.exception(err_msg)
        raise IOError(err_msg)


def get_cluster_config(config_json):
    """Get cluster-configuration as ClusterConfig object from given json
    configuration.

    :param config_json: Kafka-cluster configuration in json form
    :type config_json: json
    :returns: py:class:`yelp_kafka.config.ClusterConfig`.
    """
    try:
        return ClusterConfig(
            name=config_json['name'],
            type=config_json['type'],
            broker_list=config_json['broker_list'],
            zookeeper=config_json['zookeeper'],
        )
    except KeyError:
        err_msg = "Invalid json cluster-configuration"
        log.exception(err_msg)
        raise ConfigurationError(err_msg)


def get_local_cluster(cluster_type):
    """Get the local kafka cluster.

    :param cluster_type: kafka cluster type
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :returns: py:class:`yelp_kafka.config.ClusterConfig`
    """
    region = get_local_region()
    # TODO: PROBLEM doesn't works for devc!, overriding temporarily
    region = 'uswest1-devc'
    query = '{base_query}/clusters/{c_type}/region/{region}'.format(
        base_query=BASE_QUERY,
        c_type=cluster_type,
        region=region,
    )

    response = requests.get(query)
    status_code = response.status_code
    json_resp = requests.get(query).json()
    if status_code == 200:
        return get_cluster_config(json_resp)
    elif status_code == 404:
        err_msg = json_resp['description']
        log.exception("Local cluster fetch failed. {msg}".format(msg=err_msg))
        # TODO: any specific error-type?
        raise ConfigurationError(err_msg)
