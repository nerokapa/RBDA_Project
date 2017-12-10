import os, sys
import argparse
from math import log10
import ConfigParser
from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.ml.regression import LinearRegression
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.stat import Statistics

def model_factory(training_data, args, section, config, categorical_features_info={}):
    model = None
    maxDepth = config.getint(section, 'maxDepth')
    maxBins = config.getint(section, 'maxBins')
    if config.get(section, 'name') == 'RandomForest':
        numTrees = config.getint(section, 'numTrees')
        model = RandomForest.trainRegressor(training_data, categoricalFeaturesInfo=categorical_features_info,
                                            numTrees=numTrees, featureSubsetStrategy="auto",
                                            impurity='variance', maxDepth=maxDepth, maxBins=maxBins)
    # https://spark.apache.org/docs/2.2.0/api/python/pyspark.mllib.html#pyspark.mllib.tree.GradientBoostedTreesModel
    elif config.get(section, 'name') == 'GradientBoostedTrees':
        numIterations = config.getint(section, 'numIterations')
        loss = config.get(section, 'loss')
        model = GradientBoostedTrees.trainRegressor(training_data,
                                            categoricalFeaturesInfo=categorical_features_info, numIterations=numIterations,
                                            loss=loss , maxDepth=maxDepth, maxBins=maxBins)
    return model


def parse(lp):
    # label = float(lp[ : lp.find(' ')])
    # vec = Vectors.dense(lp[lp.find(' ') + 1: ].split(' '))
    values = [float(x) for x in lp.split(',')]
    if(values[0] < 5000000):
         return LabeledPoint(-1, [0] * (len(values) - 1))
    # import pdb; pdb.set_trace()
    return LabeledPoint(values[0], [values[1 : ]])
    # return LabeledPoint(values[0], [values[1]])

def evaluation(model, test_data, verbose=False, output=None):
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
    if output or verbose:
        glod_predict_pair = labelsAndPredictions.collect()
        glod_predict_pair = sorted(glod_predict_pair, key=lambda tup: tup[0])
        for item in glod_predict_pair:
            if verbose:
                print(item + (log10(item[0] / item[1]), ))
            if output:
                with open(output, 'a+') as fout:
                    fout.write(str(item) + '\n')

# trainingData = ssc.textFileStream("/training/data/dir").map(parse).cache()
# testData = ssc.textFileStream("/testing/data/dir").map(parse)

def Prep_data_data_base():
    pass

def Prep_data_file(args, verbose=False):
    data = sc.textFile(args.data)
    parsed_data = data.map(parse)
    parsed_data = parsed_data.filter(lambda x : x.label > 0)
    if verbose:
        parsed_data_mem = parsed_data.collect()
        for line in parsed_data_mem:
            print(str(line) + '\n')
    # random split is problematic
    training_data, val_data, test_data = parsed_data.randomSplit([0.7, 0.15, 0.15])
    return training_data, val_data, test_data

def get_cata_dict(config):
    res = {}
    for x in range(config.getint('data', 'cate_range_begin'),
                    config.getint('data', 'cate_range_end')):
        res[x] = 2
    res[config.getint('data', 'year_index')] = config.getint('data', 'year_max') - \
                                               config.getint('data', 'year_min') + 1
    return res


sc = SparkContext('local', 'prediction_model')

# test_ratio = 0.2;

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ML model for predicting the box offic')
    parser.add_argument('-data', dest="data", required=True,
                        help = "The file that contain all the data")
    parser.add_argument('-o', dest='output', default=None,
                        help = "Output file path")
    parser.add_argument('-mode', dest='mode', default='train',
                        help = "model running mode default: train or test")
    parser.add_argument('-config', dest='cfg_file', default='model_config.cfg',
                        help = "model configer file path")
    parser.add_argument('-model', dest='model_path', default=None)
    parser.add_argument('-v', dest='verbose', action='store_true')

    args = parser.parse_args()
    config = ConfigParser.ConfigParser()
    config.read(args.cfg_file)

    training_data, val_data, test_data = Prep_data_file(args)

    if args.mode == 'train':
        for section in config.sections():
            if section == 'data':
                continue
            model = model_factory(training_data, args, section, config,
                          get_cata_dict(config))
            if config.getboolean(section, 'save'):
                model.save(sc, args.model_path + section)
            evaluation(model, val_data, args.verbose, output=args.output + section)

    elif args.mode == 'test':
        for section in config.sections():
            if section == 'data':
                continue
            if 'RandomForest' in section:
                model  = RandomForestModel.load(sc, args.model_path)
            elif 'GradientBoostedTrees' in section:
                model = GradientBoostedTreesModel.load(sc, output=args.output + section)
            else:
                raise Exception('Illeagle model')
            evaluation(model, test_data, args.verbose)
