import contextlib
import mock
from StringIO import StringIO


MOCK_TOPOLOGY_CONFIG = """
---
  clusters:
    cluster1:
      broker_list:
        - "mybrokerhost1:9092"
      zookeeper_cluster: myzookeepercluster1
    cluster2:
      broker_list:
        - "mybrokerhost2:9092"
      zookeeper_cluster: myzookeepercluster2
    cluster3:
      broker_list:
        - "mybrokerhost3:9092"
        - "mybrokerhost4:9092"
      zookeeper_cluster: myzookeepercluster1
    cluster4:
      broker_list:
        - "mybrokerhost5:9092"
      zookeeper_cluster: myzookeepercluster3
  region_to_cluster:
    sfo12-prod:
      - cluster1
      - cluster3
    dc6-prod:
      - cluster1
      - cluster3
    sf-devc:
      - cluster2
    uswest1-devc:
      - cluster4
"""

MOCK_ZOOKEEPER_TOPOLOGY_CLUSTER1 = """
---
  - - "0.1.2.3"
    - 2181
  - - "0.2.3.4"
    - 2181
"""

MOCK_ZOOKEEPER_TOPOLOGY_CLUSTER2 = """
---
  - - "0.3.4.5"
    - 2181
  - - "0.6.7.8"
    - 2181
"""

MOCK_REGION = 'my-ecosystem'

TEST_BASE_ZK = '/my/zookeeper'
TEST_BASE_KAFKA = '/my/kafka'

PATH_ZK_1 = "{0}/myzookeepercluster1.yaml".format(TEST_BASE_ZK)
PATH_ZK_2 = "{0}/myzookeepercluster2.yaml".format(TEST_BASE_ZK)
PATH_KAFKA = "{0}/mykafka.yaml".format(TEST_BASE_KAFKA)
PATH_REGION = "/nail/etc/region"


PATH_TO_MOCK = {
    PATH_ZK_1: MOCK_ZOOKEEPER_TOPOLOGY_CLUSTER1,
    PATH_ZK_2: MOCK_ZOOKEEPER_TOPOLOGY_CLUSTER2,
    PATH_KAFKA: MOCK_TOPOLOGY_CONFIG,
    PATH_REGION: MOCK_REGION
}


@contextlib.contextmanager
def mock_conf_files():
    with contextlib.nested(
        mock.patch('__builtin__.open', _mock_conf_file_open),
        mock.patch('os.path.isfile', lambda x: x in PATH_TO_MOCK)
    ):
        yield


def _mock_conf_file_open(fname, mode='r'):
    stio = StringIO()
    if fname in PATH_TO_MOCK:
        stio.write(PATH_TO_MOCK[fname])
    else:
        raise IOError('This file {0} does not exist in the mock'.format(fname))
    stio.seek(0)
    return contextlib.closing(stio)
