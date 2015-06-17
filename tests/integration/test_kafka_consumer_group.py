def test_foobar():

    import os
    print "About to create topic"
    os.system('/usr/bin/kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic test')

    print "About to list topics"
    os.system('/usr/bin/kafka-topics --list --zookeeper zookeeper:2181')

    assert False

