#!/usr/bin/env bash

set -e

BIN_DIR="/opt/kafka_2.10-0.8.2.1/bin"
CONF_DIR="/opt/kafka_2.10-0.8.2.1/config"

ZK_LOG="/work/zk.log"
KAKFA_LOG="/work/kafka.log"

echo "Starting zookeeper ..."
"${BIN_DIR}/zookeeper-server-start.sh ${CONF_DIR}/zookeeper.properties > ${ZK_LOG} 2>&1"
sleep 3
echo "Starting kafka ..."
"${BIN_DIR}/kafka-server-start.sh ${CONF_DIR}/server.properties > ${KAFKA_LOG} 2>&1"
sleep 3
echo "Ready to go"

