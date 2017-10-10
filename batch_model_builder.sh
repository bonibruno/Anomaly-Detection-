#!/bin/sh
# Run this only on one site.

cd "${0%/*}"
source ./config.sh

#export ENRICHED_DATA_PATH=${GLOBAL_ROOT}/tmp/ecs_iot_demo2/enriched_data_one_sequential_pass

#export MODEL_PATH=${GLOBAL_ROOT}/tmp/ecs_iot_demo2/model
export MODEL_PATH=${GLOBAL_ROOT}/tmp/ecs_iot_demo2/model_throwaway

hadoop fs -rm -r -skipTrash ${MODEL_PATH}.tmp

if time spark-submit \
	--master yarn \
	--deploy-mode client \
	--driver-memory 4g \
	--num-executors 4 \
	--executor-cores 2 \
	--executor-memory 8g \
	--packages com.databricks:spark-csv_2.10:1.4.0 \
	batch_model_builder.py \
	--input_data_path ${ENRICHED_DATA_PATH}/*/* \
	--data_format json \
	--model_path ${MODEL_PATH}.tmp \
; then
	hadoop fs -rm -r ${MODEL_PATH}
	hadoop fs -mv ${MODEL_PATH}.tmp ${MODEL_PATH}
	hadoop fs -ls -h -R ${MODEL_PATH}
else
	echo ERROR!
fi
