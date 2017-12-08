import os, sys
import argparse

import numpy as np

from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.ml.regression import LinearRegression
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils

from pyspark.mllib.stat import Statistics

def parse(lp):
    # label = float(lp[ : lp.find(' ')])
    # vec = Vectors.dense(lp[lp.find(' ') + 1: ].split(' '))
    values = [float(x) for x in lp.split(' ')]
    if(values[0] < 5000000):
         return LabeledPoint(-1, [-1, -1])
    # import pdb; pdb.set_trace()
    return LabeledPoint(values[0], [values[1], values[3]])
    # return LabeledPoint(values[0], [values[1]])


def model_factory(training_data, model_name='RandomForest', categorical_features_info={}):
    model = None
    if model_name == 'RandomForest':
        model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo=categorical_features_info,
                                            numTrees=10, featureSubsetStrategy="auto",
                                            impurity='variance', maxDepth=20, maxBins=64)
    # https://spark.apache.org/docs/2.2.0/api/python/pyspark.mllib.html#pyspark.mllib.tree.GradientBoostedTreesModel
    elif model_name == 'GradientBoostedTrees':
        model = GradientBoostedTrees.trainRegressor(trainingData,
                                            categoricalFeaturesInfo={}, numIterations=100)
    elif model_name == 'LinearRegressionWithSGD':
        # this model do not work good
        # depend highly on the initial value
        model = LinearRegressionWithSGD.train(parsed_data, iterations=10000, step=0.00001,
                                          miniBatchFraction = 1.0)


def evaluation(model, test_data, verbose=False):
    predictions = model.predict(test_data.map(lambda x: x.features))
    gold = test_data.map(lambda x : x.label)
    labelsAndPredictions = test_data.map(lambda lp: lp.label).zip(predictions)
    test_percentage_err_mean = labelsAndPredictions.map(lambda lp: abs(lp[0] - lp[1]) / lp[0]).sum() /\
                                                            float(labelsAndPredictions.count())
    test_error_sqaure_mean= labelsAndPredictions.map(lambda lp: (lp[0] - lp[1]) * (lp[0] - lp[1])).sum() /\
                                                            float(labelsAndPredictions.count())
    print("THe pearson corrolation equals:")
    print(Statistics.corr(labelsAndPredictions, method="pearson"))
    print('Test Mean Squared Error = ' + str(test_error_sqaure_mean))
    print('Test Mean Precentage Error = ' + str(test_percentage_err_mean))
    if verbose:
        glod_predict_pair = labelsAndPredictions.collect()
        glod_predict_pair = sorted(glod_predict_pair, key=lambda tup: tup[0])
        for item in glod_predict_pair:
            # if(abs(item[0] - item[1]) / item[0] > 10):
                print(item + (abs(item[0] - item[1]) / item[0], ))

# trainingData = ssc.textFileStream("/training/data/dir").map(parse).cache()
# testData = ssc.textFileStream("/testing/data/dir").map(parse)



sc = SparkContext('local', 'regression_model')

# test_ratio = 0.2;

if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description = '')
    # parser.add_argument("-model", dest="model_name", required = True,
    #                     help= "The name of the regression model to use")
    # parser.add_argument("-train", dest="-training_data", required = True,
    #                     help = "The file that contain")
    # parser.add_argument("-s", dest="source", required = True,
    #                     help = "the folder that contains the .c files of students" )
    # args = parser.parse_args()
    data = sc.textFile(sys.argv[1])
    parsed_data = data.map(parse)
    parsed_data.filter(lambda x : x.lable > 0)
    # view_training_data = parsed_data.collect()
    # for data in view_training_data:
    #     print(str(data))

    (training_data, test_data) = parsed_data.randomSplit([0.7, 0.3])
    model = RandomForest.trainRegressor(training_data, categoricalFeaturesInfo={},
                                                numTrees=20, featureSubsetStrategy="auto",
                                                impurity='variance', maxDepth=20, maxBins=64)
    evaluation(model, test_data)
    # print('Learned regression GBT model:')
    # print(model.toDebugString())
    # vp = valuesAndPreds.collect()
    # # import pdb; pdb.set_trace()
    # for (v, p) in vp:
    #     print("the gold is : " + str(v) + "  the predict is : " + str(p))
    # MSE = valuesAndPreds.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / valuesAndPreds.count()
    # print("Mean Squared Error = " + str(MSE))
