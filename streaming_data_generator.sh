#!/bin/sh

cd "${0%/*}"
source ./config.sh

KAFKA_HOST=`hostname -f`

./streaming_data_generator.py \
--kdd_file /mnt/home/faheyc/demos/ecs_iot_demo2/data/kdd/kddcup.data/kddcup.data \
--kafka_zookeeper_hosts ${KAFKA_HOST}:2181 \
--kafka_broker_list ${KAFKA_HOST}:6667 \
--kafka_message_topic ${KAFKA_MESSAGE_TOPIC} \
--throttle_messages_per_sec 100
