import os, sys
from pyspark import SparkContext
from pyspark.sql import SQLContext

from __future__ import print_function

from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import StreamingLinearRegressionWithSGD

# lacking the code to link the hbase to the spark mllib model

sc = SparkContext()
sqlc = SQLContext(sc)
data_source_format = 'org.apache.hadoop.hbase.spark'
df = sc.parallelize([('a', '1.0'), ('b', '2.0')]).toDF(schema=['col0', 'col1'])

HBASE_DOMAIN = os.getenv('HBASE_DOMAIN', 'localhost')
MOVIE_DATA_TABLE = os.getenv('MOVIE_DATA_TABLE', 'MovieTable')
# CAST_DATA_TABLE = os.getenv('CAST_DATA_TABLE', 'CastTable')

MOVIE_ATTR = ["budget", "genre", "revenue", "title", "year"]

catalog = ''.join("""{
    "table":{"namespace":"default", "name":"testtable"},
    "rowkey":"key",
    "columns":{
        "col0":{"cf":"budget", "col":"key", "type":"string"},
        "col1":{"cf":"genre", "col":"col1", "type":"string"},
        "col2":{"cf":"revenue", "col":"col2", "type":"string"},
        "col3":{"cf":"title", "col":"col3", "type":"string"},
        "col4":{"cf":"year", "col":"col4", "type":"string"},
    }
}""".split())

# Reading
df = sqlc.read\
.options(catalog=catalog)\
.format(data_source_format)\
.load()

# currencly using the local file to test
# we will use the spark streaming to get data stream from the hbase to feed the online training algorithm
trainingData = ssc.textFileStream(sys.argv[1]).map(parse).cache()
# testData = ssc.textFileStream(sys.argv[2]).map(parse)

numFeatures = 3
model = StreamingLinearRegressionWithSGD()
model.setInitialWeights([0.0, 0.0, 0.0])
model.trainOn(trainingData)
print(model.predictOnValues(testData.map(lambda lp: (lp.label, lp.features))))
ssc.start()
ssc.awaitTermination()
