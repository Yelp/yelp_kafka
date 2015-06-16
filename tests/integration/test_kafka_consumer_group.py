import subprocess
import kafka

cmd = '/work/start_kafka.sh start'
child = subprocess.Popen(cmd, bufsize=1, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, shell=True)

while True:
    output = child.stdout.readline()
    if "KAFKA STARTED" in output:
        break
    else:
        print "~>", output

print "done!"

import os
os.system('/opt/kafka_2.10-0.8.2.1/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test222')
os.system('/opt/kafka_2.10-0.8.2.1/bin/kafka-topics.sh --list --zookeeper localhost:2181')

from kafka import SimpleProducer, KafkaClient, KafkaConsumer

# To send messages synchronously
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)

# Note that the application is responsible for encoding messages to type bytes
producer.send_messages('test222', 'some message')
print "sent a message"

# consumer = KafkaConsumer('test222', group_id='my_group',
#         metadata_broker_list=['localhost:9092'])
# for message in consumer:
#     # message value is raw byte string -- decode if necessary!
#     # e.g., for unicode: `message.value.decode('utf-8')`
#     print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
#                                          message.offset, message.key,
#                                          message.value))
#     break

cmd = '/work/start_kafka.sh stop'
child_2 = subprocess.Popen(cmd, bufsize=1, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, shell=True)

while True:
    output = child_2.stdout.readline()
    if "KAFKA STOPPED" in output:
        break

print "bye bye!"

raise Exception("")
