#!/bin/sh

case ${HOSTNAME} in
	hop-claudio-hdp3-mini-0.solarch.lab.emc.com)
		export SITENUM=1
		;;
	hop-claudio-hdp4-mini-0.solarch.lab.emc.com)
		export SITENUM=2
		;;
	*)
		exit 1
		;;
esac

export PATH=/opt/anaconda/bin:$PATH
export GLOBAL_ROOT=viprfs://repbucket1.ns1.site${SITENUM}
export KAFKA_MESSAGE_TOPIC=sensor_messages
export KAFKA_ALERT_TOPIC=alerts
export KAFKA_ENRICHED_DATA_TOPIC=enriched_data
export ENRICHED_DATA_PATH=${GLOBAL_ROOT}/tmp/ecs_iot_demo2/enriched_data
#export ENRICHED_DATA_PATH=${GLOBAL_ROOT}/tmp/ecs_iot_demo2/enriched_data_one_sequential_pass
export MODEL_PATH=${GLOBAL_ROOT}/tmp/ecs_iot_demo2/model

echo SITENUM: ${SITENUM}
echo GLOBAL_ROOT: ${GLOBAL_ROOT}
echo ENRICHED_DATA_PATH: ${ENRICHED_DATA_PATH}
