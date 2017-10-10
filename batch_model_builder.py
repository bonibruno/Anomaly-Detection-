#
# PySpark batch job that trains a machine learning model on the entire global dataset.
# The model is then saved to disk for use by message_streaming_processor.py.
#
# Run with: spark-submit --master yarn-client --packages com.databricks:spark-csv_2.10:1.4.0 batch_model_builder.py ...
#
# Written by: claudio.fahey@emc.com
#

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.ml.param import Param, Params
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from optparse import OptionParser
from common import get_kdd_schema_text
from common_pyspark import build_features_vector, extract_features, extract_labeled_points

def get_kdd_csv_dataframe(sqlContext, input_data_path):
    """Read KDD data in original CSV format."""
    schema_text = get_kdd_schema_text()
    type_map = {'continuous.': FloatType(), 'symbolic.': IntegerType(), 'string.': StringType()}
    cols = [StructField(x.split(': ')[0], type_map.get(x.split(': ')[1], StringType()), True) for x in schema_text.split('\n')]
    schema = StructType(cols)

    df = (sqlContext
        .read.format('com.databricks.spark.csv')
        .option('header', 'false')
        .option('mode', 'DROPMALFORMED')
        .load(input_data_path, schema=schema))
    return df

def unisample(df, fraction=1.0):
    """From http://info.mapr.com/rs/mapr/images/Getting_Started_With_Apache_Spark.pdf section Unsupervised Anomaly Detection with Spark."""
    columns = df.first()
    new_df = None
    for i in range(0, len(columns)):
        column = df.sample(withReplacement=True, fraction=fraction) \
            .map(lambda row: row[i]) \
            .zipWithIndex() \
            .map(lambda e: (e[1], [e[0]]))
        if new_df is None:
            new_df = column
        else:
            new_df = new_df.join(column)
            new_df = new_df.map(lambda e: (e[0], e[1][0] + e[1][1]))
    return new_df.map(lambda e: e[1])

def supervised2unsupervised(model, fraction=1.0):
    """Converts a supervised model that takes labeled data to an unsupervised data that takes unlabeled data.
    Points that are similar to the training data are predicted to have the label 1 and anomalies will have the label 0.
    From http://info.mapr.com/rs/mapr/images/Getting_Started_With_Apache_Spark.pdf section Unsupervised Anomaly Detection with Spark."""
    def run(df, *args, **kwargs):
        unisampled_df = unisample(df, fraction=fraction)
        labeled_data = df.map(lambda e: LabeledPoint(1, e)) \
            .union(unisampled_df.map(lambda e: LabeledPoint(0, e)))
        return model(labeled_data, *args, **kwargs)
    return run

def main(sc):
    parser = OptionParser()
    parser.add_option('', '--input_data_path', action='store', dest='input_data_path', help='path for input data')
    parser.add_option('', '--model_path', action='store', dest='model_path', help='path for model data')
    parser.add_option('', '--data_format', default='json', action='store', dest='data_format', help='format of input data (json, csv)')
    options, args = parser.parse_args()

    sqlContext = SQLContext(sc)

    if options.data_format == 'json':
        df = sqlContext.read.json(options.input_data_path)
    elif options.data_format == 'csv':
        df = get_kdd_csv_dataframe(sqlContext, options.input_data_path)
    else:
        raise Exception('Unknown data format')

    # Drop duplicate records based on uuid.
    # Duplicate records may be created due to various failure conditions in Spark Streaming, Kafka, or Flume.
    # Although duplicate records may not have a significant impact with Random Forest, we remove them here
    # in case we use another algorithm that is more sensitive to them.
    df = df.dropDuplicates(['uuid'])

    # Build feature vector.
    df = build_features_vector(df)

    # Show feature vector.
    # df.select([df['features']]).show(100)
    # print(df.select([df['features']]).rdd.collect())

    # Train model.
    # We must use RDDs, not dataframes, because we can't save/load the pipelined ML model using PySpark yet.
    # The best parameters for training should be determined using cross validation but that is not done in this demo.
    features_rdd = extract_features(df)
    unsupervised_forest = supervised2unsupervised(RandomForest.trainClassifier, fraction=0.1)
    model = unsupervised_forest(features_rdd, 
        numClasses=2,
        categoricalFeaturesInfo={},
        numTrees=10, 
        featureSubsetStrategy='auto', 
        impurity='gini',
        maxDepth=15, 
        maxBins=50)

    # Save model to disk.
    model.save(sc, options.model_path)

if __name__ == '__main__':
    sc = SparkContext()
    main(sc)
