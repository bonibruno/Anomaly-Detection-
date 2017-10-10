#
# PySpark Streaming job that receives messages from Kafka, enriches the data, persists to storage, performs anomaly detection,
# and reports anomalies to a Kafka topic.
#
# Run with: spark-submit --master yarn-client --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0 message_streaming_processor.py
#
# Written by: claudio.fahey@emc.com
#

from __future__ import division
from __future__ import print_function
from pyspark.mllib.linalg import Vectors
from pyspark.sql import SQLContext, Row
from pyspark import SparkContext, RDD
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.util import rddToFileName
from pykafka import KafkaClient
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml import Pipeline
from time import sleep
from optparse import OptionParser
import json
import logging
from common_pyspark import build_features_vector, extract_features
from common import LOGGING_FORMAT

def getSqlContextInstance(sparkContext):
    """Lazily instantiated global instance of SQLContext
    Below from https://spark.apache.org/docs/1.5.2/streaming-programming-guide.html#dataframe-and-sql-operations."""
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def json_to_row(s):
    return Row(**json.loads(s))

def enrich_data(df):
    """Enrich the incoming data."""
    df = df.withColumn('total_bytes', df.src_bytes + df.dst_bytes)
    return df

def write_partition_to_kafka(df, zookeeper_hosts, kafka_topic):
    """Write a partition of a dataframe to Kafka.
    This runs in the worker proceses."""
    # We must start our own logging for this worker process.
    # We will also see PyKafka messages in this log.
    logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
    logger = logging.getLogger('write_partition_to_kafka')
    logger.info('BEGIN')
    client = KafkaClient(zookeeper_hosts=zookeeper_hosts)
    topic = client.topics[kafka_topic]
    # Note that if we used the default linger_ms=5000, there would be a minimum delay of 10 seconds to
    # complete this function. With linger_ms=1, the delay is 5-10 milliseconds.
    with topic.get_producer(delivery_reports=False, linger_ms=10.0) as producer:
        logger.info('Kafka client connected')
        for row in df:
            msg = json.dumps(row.asDict())
            producer.produce(msg)
        logger.info('Produced all messages')
    logger.info('END')

BATCH_COUNTER = 0   # global variable to track the number of streaming batches that have been processed so far

def process_messages(time, rdd, ssc, model, enriched_data_path, zookeeper_hosts, kafka_alert_topic, kafka_enriched_data_topic, max_batches):
    global BATCH_COUNTER

    if rdd.isEmpty():
        return

    sqlContext = getSqlContextInstance(rdd.context)
    df = sqlContext.createDataFrame(rdd)

    # Enrich data to build preprocesed dataframe.
    df = enrich_data(df)

    # Perist enriched data to storage (direct from Spark to HDFS).
    # This will create a file per partition per batch.
    if enriched_data_path:
        path = rddToFileName(enriched_data_path, None, time)
        df.write.json(path, mode='error')
    
    # Send all enriched data to a Kafka topic.
    # Note that each worker sends its own partitions directly to Kafka. The driver is not in the data path.
    # This can be consumed by Flume to write to HDFS allowing multiple batches to be appended to the same file.
    if kafka_enriched_data_topic:
        df.foreachPartition(lambda d: write_partition_to_kafka(d, zookeeper_hosts=zookeeper_hosts, kafka_topic=kafka_enriched_data_topic))

    # Build feature vector.
    df = build_features_vector(df)

    # Show 10 records of the dataframe.
    # df.select(['duration','src_bytes','dst_bytes','features','label']).show(10)

    # Predict anomalies with model.
    # We must use RDDs, not dataframes, because we can't save/load the pipelined ML model using PySpark yet.
    if model:
        features_rdd = extract_features(df)
        predictions_rdd = model.predict(features_rdd)
        features_and_predictions_rdd = df.rdd.zip(predictions_rdd)
        anomalies_rdd = features_and_predictions_rdd.filter(lambda x: x[1] <= 0).map(lambda x: x[0])
        anomalies = anomalies_rdd.collect()
        print('Predicted %d anomalies' % len(anomalies))

        # For demo purposes, only alert on the first 5 anomalies.
        anomalies = anomalies[:5]

        # Send anomalies to Kafka.
        # Note that since we expect very few anomalies, the records are brought into the driver which
        # then sends to Kafka.
        if anomalies:
            client = KafkaClient(zookeeper_hosts=zookeeper_hosts)
            topic = client.topics[kafka_alert_topic]
            with topic.get_producer(delivery_reports=False) as producer:
                for row in anomalies:
                    alert = row.asDict()
                    del alert['features']       # remove features vector because we can't serialize it to JSON
                    alert['alert_text'] = 'predicted to be an anomaly'
                    msg = json.dumps(alert)
                    producer.produce(msg)
                    print('Sent alert: %s' % msg)

    # Stop after specified number of batches. This is used for development only.
    BATCH_COUNTER += 1
    if max_batches > 0 and BATCH_COUNTER >= max_batches:
        print('Reached maximum number of batches.')
        ssc.stop(True, False)

def main():
    parser = OptionParser()
    parser.add_option('', '--enriched_data_path', action='store', dest='enriched_data_path', help='path to write enriched data')
    parser.add_option('', '--model_path', action='store', dest='model_path', help='path for model data')
    parser.add_option('', '--kafka_zookeeper_hosts', action='store', dest='kafka_zookeeper_hosts', help='list of Zookeeper hosts (host:port)')
    parser.add_option('', '--kafka_broker_list', action='store', dest='kafka_broker_list', help='list of Kafka brokers (host:port)')
    parser.add_option('', '--kafka_message_topic', action='store', dest='kafka_message_topic', help='topic to consume input messages from')
    parser.add_option('', '--kafka_alert_topic', action='store', dest='kafka_alert_topic', help='topic to produce alert messages to')
    parser.add_option('', '--kafka_enriched_data_topic', action='store', dest='kafka_enriched_data_topic', help='topic to produce enriched data to')
    parser.add_option('', '--streaming_batch_duration_sec', type='float', default=15.0,
        action='store', dest='streaming_batch_duration_sec', help='Streaming batch duration in seconds')
    parser.add_option('', '--max_batches', type='int', default=0,
        action='store', dest='max_batches', help='Number of batches to process (0 means forever)')
    options, args = parser.parse_args()

    sc = SparkContext()
    ssc = StreamingContext(sc, options.streaming_batch_duration_sec)
    sqlContext = getSqlContextInstance(sc)

    # Load saved model.
    model = None
    if options.model_path:
        model = RandomForestModel.load(sc, options.model_path)
    else:
        print('No model loaded.')

    # Create Kafka stream to receive new messages.
    kvs = KafkaUtils.createDirectStream(ssc, [options.kafka_message_topic], {
        'metadata.broker.list': options.kafka_broker_list,
        'group.id': 'spark_streaming_processor.py'})

    # Take only the 2nd element of the tuple.
    messages = kvs.map(lambda x: x[1])

    # Convert RDD of JSON strings to RDD of Rows.
    rows = messages.map(json_to_row)

    # Process messages.
    rows.foreachRDD(lambda time, rdd: 
        process_messages(time, rdd,
            ssc=ssc,
            model=model,
            enriched_data_path=options.enriched_data_path,
            zookeeper_hosts=options.kafka_zookeeper_hosts,
            kafka_alert_topic=options.kafka_alert_topic,
            kafka_enriched_data_topic=options.kafka_enriched_data_topic,
            max_batches=options.max_batches))

    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()
