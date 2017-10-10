#!/bin/sh
# Run this on all sites.

cd "${0%/*}"
source ./config.sh

KAFKA_HOST=`hostname -f`

# Only use the predictive model if it has been created.
MODEL_ARG=
hadoop fs -test -d ${MODEL_PATH} && MODEL_ARG="--model_path ${MODEL_PATH}"

# Use deploy-mode client to view the driver output in the console.
# Use deploy-mode cluster to run the driver in YARN.

if spark-submit \
	--master yarn \
	--deploy-mode client \
	--num-executors 2 \
	--executor-cores 1 \
	--packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0 \
	spark_streaming_processor.py \
	--kafka_zookeeper_hosts ${KAFKA_HOST}:2181 \
	--kafka_broker_list ${KAFKA_HOST}:6667 \
	--kafka_message_topic ${KAFKA_MESSAGE_TOPIC} \
	--kafka_alert_topic ${KAFKA_ALERT_TOPIC} \
	--kafka_enriched_data_topic ${KAFKA_ENRICHED_DATA_TOPIC} \
	--streaming_batch_duration_sec 15 \
	${MODEL_ARG} \
; then
	# Command returned successfully.
    hadoop fs -ls -h ${ENRICHED_DATA_PATH}
else
	echo ERROR!
	# Wait here for a long time so we can view the error on the screen.
	sleep 1d
fi
