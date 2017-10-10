from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.mllib.regression import LabeledPoint
from common import get_kdd_feature_cols

def build_features_vector(df, featuresCol='features'):
	"""Build the feature vector. To simplify, we only consider the numeric features. For a more accurate model, we should
	encode the categorical features and use them as well."""
	inputCols=get_kdd_feature_cols()
	assembler = VectorAssembler(inputCols=inputCols, outputCol=featuresCol)
	return assembler.transform(df)

def extract_features(df, featuresCol='features'):
    return df.select(featuresCol).map(lambda row: row.asDict()[featuresCol])

def extract_labeled_points(df, labelCol, featuresCol):
    """Based on https://github.com/apache/spark/blob/098be27ad53c485ee2fc7f5871c47f899020e87b/mllib/src/main/scala/org/apache/spark/ml/Predictor.scala#L123"""
    return df.select(labelCol, featuresCol).map(lambda row: LabeledPoint(row.asDict()[labelCol], row.asDict()[featuresCol]))
