import logging
import requests
from yelp_kafka.config import ClusterConfig
from yelp_kafka.error import ConfigurationError
from yelp_kafka.error import DiscoveryError

REGION_FILE_PATH = '/nail/etc/region'
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


def as_cluster_config(config_json):
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
    region = 'uswest1-devc' if region == 'sf-devc' else region
    query = '{base_query}/clusters/{c_type}/region/{region}'.format(
        base_query=BASE_QUERY,
        c_type=cluster_type,
        region=region,
    )
    return as_cluster_config(execute_query(query, err_msg="Cluster config fetch failed."))


def execute_query(query, raise_exception=DiscoveryError, err_msg=None):
    response = requests.get(query)
    status_code = response.status_code
    json_resp = response.json()
    if status_code == 200:
        return json_resp
    elif status_code == 404:
        error = json_resp['description']
        log.exception("{error}.{err_msg}".format(error=error, err_msg=err_msg))
        raise raise_exception(err_msg)


def get_all_clusters(cluster_type):
    """Get a list of (cluster_name, cluster_config)
    for the available kafka clusters in the ecosystem.

    :param cluster_type: kafka cluster type
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :returns: list of py:class:`yelp_kafka.config.ClusterConfig`
    """
    query = '{base_query}/clusters/{c_type}/named'.format(
        base_query=BASE_QUERY,
        c_type='abc',
    )
    kafka_clusters = execute_query(query, err_msg="Kafka clusters fetch failed.")

    cluster_configs = []
    for cluster in kafka_clusters:
        query = '{base_query}/clusters/{c_type}/named/{named}'.format(
            base_query=BASE_QUERY,
            c_type=cluster_type,
            named=cluster,
        )
        cluster_configs.append(
            as_cluster_config(
                execute_query(query, err_msg="Cluster config fetch failed."),
            ),
        )
    return cluster_configs


def get_cluster_by_name(cluster_type, cluster_name):
    """Get a :py:class:`yelp_kafka.config.ClusterConfig` from an ecosystem with
    a particular name.

    :param cluster_type: kafka cluster type
        (ex.'scribe' or 'standard').
    :type cluster_type: string
    :param cluster_name: name of the cluster
        (ex.'uswest1-devc').
    :type cluster_type: string
    :returns: :py:class:`yelp_kafka.config.ClusterConfig`
    """
    # TODO: double check query
    # cluster-name is kafka-cluster-name
    query = '{base_query}/clusters/{c_type}/named/{named}'.format(
        base_query=BASE_QUERY,
        c_type=cluster_type,
        named=cluster_name,
    )
    return as_cluster_config(
        execute_query(query, err_msg="Cluster-config fetch failed."),
    )
