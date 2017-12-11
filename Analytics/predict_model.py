import os, sys
import argparse
from math import log10
import ConfigParser
import json

from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.ml.regression import LinearRegression
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.stat import Statistics

from data_integration import all_movies

sc = SparkContext('local', 'prediction_model')

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
    else:
        raise Exception('Illeagle model')
    return model

# this function will not be used in the future
def parse(lp):
    # label = float(lp[ : lp.find(' ')])
    # vec = Vectors.dense(lp[lp.find(' ') + 1: ].split(' '))
    values = [float(x) for x in lp.split(',')]
    if(values[0] < 5000000):
         return LabeledPoint(-1, [0] * (len(values) - 1))
    # import pdb; pdb.set_trace()
    return LabeledPoint(values[0], [values[1 : ]])
    # return LabeledPoint(values[0], [values[1]]

# def parse_oa_statistic(lp):
#     lp = json.loads(lp)
#     map(lambda x : lp.pop(x), ['oa_screen_name', 'movie_name'])
#     movie_id = lp.pop('movie_id')
#     lp = lp.items().sort(key=lambda tup: tup[0])
#     lp = map(lambda tup : tup[1], lp)
#     return (movie_id,lp);


def evaluation(section, model, test_data, verbose=False, output=None):
    predictions = model.predict(test_data.map(lambda x: x.features))
    gold = test_data.map(lambda x : x.label)
    labelsAndPredictions = test_data.map(lambda lp: lp.label).zip(predictions)
    test_percentage_err_mean = labelsAndPredictions.map(lambda lp: abs(lp[0] - lp[1]) / lp[0]).sum() /\
                                                            float(labelsAndPredictions.count())
    test_error_sqaure_mean= labelsAndPredictions.map(lambda lp: (lp[0] - lp[1]) * (lp[0] - lp[1])).sum() /\
                                                            float(labelsAndPredictions.count())
    print("evaluating the section: " + section)
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

def Prep_data_hbase(args):
    tmdb_data = []
    for item in all_movies():
        features = [item['budget']] + [item['cast_impression']] + [item['year']]
        features = features + item['genre'] + item['lang']
        t = ()
        t = t +  (item['id'], )
        t = t + ([item['revenue']] + features, )
        tmdb_data.append(t)

    # if args.twitter_file:
    #     oa_stat_data = sc.textFile(args.twitter_file)
    #     oa_stat_data = oa_stat_data.map(parse_oa_statistic)
    #     oa_stat_data.join(tmdb_data)
    #
    #     all_data = all_d11ata.values()
    # else:
    #     all_data = tmdb_data.values()


    #split all_feature data
    pass



def Prep_data_file(args):
    data = sc.textFile(args.data)
    parsed_data = data.map(parse)
    parsed_data = parsed_data.filter(lambda x : x.label > 0)
    if args.verbose:
        parsed_data_mem = parsed_data.collect()
        for line in parsed_data_mem:
            print(str(line) + '\n')
    training_data, val_data, test_data = parsed_data.randomSplit([0.7, 0.15, 0.15])
    return training_data, val_data, test_data


def get_cata_dict(config):
    res = {}
    for x in range(config.getint('data', 'cate_range_begin'),
                    config.getint('data', 'cate_range_end')):
        res[x] = 2
    return res





if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ML model for predicting the box offic')
    parser.add_argument('-data', dest="data", default=None,
                        help = "The file that contain all the data")
    parser.add_argument('-twitter', dest='twitter_file', default=None,
                        help = 'The file to twitter official account statistics')
    parser.add_argument('-o', dest='output', default=None,
                        help = "Output file path")
    parser.add_argument('-mode', dest='mode', default='train',
                        help = "model running mode default: train or test")
    parser.add_argument('-config', dest='cfg_file', default='model_config.cfg',
                        help = "model configer file path")
    parser.add_argument('-model', dest='model_path', default=None,
                        help = "the path to save or load model")
    parser.add_argument('-v', dest='verbose', action='store_true',
                        help = 'use this option to set the flag of verbose mode')

    args = parser.parse_args()
    config = ConfigParser.ConfigParser()
    config.read(args.cfg_file)

    # loading from data is just for testing the function
    if args.data:
        training_data, val_data, test_data = Prep_data_file(args)
    else:
        training_data, val_data, test_data = Prep_data_hbase(args)

    if args.mode == 'train':
        for section in config.sections():
            if section == 'data':
                continue
            model = model_factory(training_data, args, section, config,
                          get_cata_dict(config))
            if config.getboolean(section, 'save'):
                model.save(sc, args.model_path + section)
            evaluation(section, model, val_data, args.verbose, output=args.output + section)

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
            evaluation(section, model, test_data, args.verbose)
