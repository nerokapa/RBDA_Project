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

from hbase_connection import *

sc = SparkContext('local', 'prediction_model')

file_buffer = './tmdb_cache'

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

def parse_ignore_id(lp):
    values = [float(x) for x in lp.split(' ')]
    return LabeledPoint(values[1], [values[2 : ]])

# key is id, value is a list starting with revnue
def parse_with_id(lp):
    values = [float(x) for x in lp.split(' ')]
    return (int(values[0]), values[1:])

def parse_oa_statistic(lp):
    lp = json.loads(lp)
    map(lambda x : lp.pop(x), ['oa_screen_name', 'movie_name'])
    movie_id = lp.pop('movie_id')
    lp = lp.items()
    lp.sort(key=lambda tup: tup[0])
    lp = map(lambda tup : tup[1], lp)
    return (movie_id,lp);

# mapper from (id, [revenue features]) to LabeledPoint
def tuple_to_LB(line, strip = False):
    twitter_features = line[1][0]
    tmdb_features = line[1][1][1 : ]
    revenue = line[1][1][0]
    if not strip:
        return LabeledPoint(revenue, tmdb_features + twitter_features)
    else:
        return LabeledPoint(revenue, tmdb_features)

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

def concat_feature(item, config):
    movie_id = [item['id']]
    revenue = [item['revenue']]
    # should read the config file
    # and make changes
    features = [item['budget']] + [item['cast_impression']] + [item['year']]
    features = features + item['genre'] + item['lang']
    movie = movie_id + revenue + features
    return movie

#
# def Prep_data_hbase(args, config):
#     # import pdb; pdb.set_trace()
#     oa_stat_data = None
#     #will not be from file in te future
#     if args.twitter_file:
#         oa_stat_data = sc.textFile(args.twitter_file)
#         oa_stat_data = oa_stat_data.map(parse_oa_statistic)
#
#     all_movie_rdd_list = []
#     # adding all filters
#     data_filters = [DF_numeric_WL('year', 1980, 2004)]
#     data_filters.append(DF_numeric_WL('revenue', 5000000, 1000000000000))
#
#     for item in all_movies(data_filters):
#         movie = concat_feature(item, config)
#         rdd = sc.parallelize([movie])
#         rdd = rdd.map(lambda x : (x[0], x[1:]))
#         all_movie_rdd_list.append(rdd)
#     all_movie_rdd = sc.union(all_movie_rdd_list)
#     # print(str(filtered_movies_rdd.collect()))
#     if args.twitter_file: # this will be changed in the future
#         if oa_stat_data:
#             # print(str(oa_stat_data.collect()))
#             filtered_movies_rdd = all_movie_rdd.join(oa_stat_data)
#         else:
#             raise Exception("Twitter data not loaded.")
#     # import pdb; pdb.set_trace()
#     # major, minor = filtered_movies_rdd.randomSplit([0.99, 0.01])
#     # print(str(minor.collect()))
#     pass


def hbase_data2file(args):
    data_filters = [DF_numeric_WL('year', 2006, 2017)]
    data_filters.append(DF_numeric_WL('revenue', 5000000, 1000000000000))

    for item in all_movies(data_filters):
        line = concat_feature(item, args)
        line = ' '.join([str(x) for x in line])
        with open(args.file_buffer, 'a+') as ofile:
            ofile.write(line + '\n')


def Prep_data_file(args):
    hbase_data2file(args)
    data = sc.textFile(args.file_buffer)
    parsed_data = None
    if not args.twitter_file:
        parsed_data = data.map(parse_ignore_id)
    else:
        parsed_data_k_id = data.map(parse_with_id)
        oa_stat_data = sc.textFile(args.twitter_file)
        oa_stat_data = oa_stat_data.map(parse_oa_statistic)
        parsed_data = oa_stat_data.join(parsed_data_k_id)
        if not args.strip:
            parsed_data = parsed_data.map(tuple_to_LB)
        else:
            parsed_data = parsed_data.map(lambda x : tuple_to_LB(x, True))
    if args.verbose:
        parsed_data_mem = parsed_data.collect()
        for line in parsed_data_mem:
            print(str(line) + '\n')
    training_data, test_data = parsed_data.randomSplit([0.7, 0.3])
    return training_data, test_data


def get_cata_dict(config):
    res = {}
    for x in range(config.getint('data', 'cate_range_begin'),
                    config.getint('data', 'cate_range_end')):
        res[x] = 2
    return res


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ML model for predicting the box offic')
    parser.add_argument('-file_buffer', dest="file_buffer", default=file_buffer,
                        help = "The file that contain all the data")
    parser.add_argument('-twitter', dest='twitter_file', default=None,
                        help = 'The file to twitter official account statistics')
    parser.add_argument('-o', dest='output', default=None,
                        help = "Output file path")
    parser.add_argument('-config', dest='cfg_file', default='model_config.cfg',
                        help = "model configer file path")
    parser.add_argument('-model', dest='model_path', default=None,
                        help = "the path to save or load model")
    parser.add_argument('-v', dest='verbose', action='store_true',
                        help = 'use this option to set the flag of verbose mode')
    parser.add_argument('-stript_twitter', dest='strip', action='store_true')

    args = parser.parse_args()
    config = ConfigParser.ConfigParser()
    config.read(args.cfg_file)

    if os.path.isdir(args.output):
        os.system('rm -rf ' + args.output)
    if os.path.isdir(args.model_path):
        os.system('rm -rf ' + args.model_path)

    os.makedirs(args.output)
    os.makedirs(args.model_path)

    if args.file_buffer:
        training_data, test_data = Prep_data_file(args)
    else:
        training_data, test_data  = Prep_data_hbase(args, config)

    for section in config.sections():
        if 'data' in section:
            continue
        model = model_factory(training_data, args, section, config,
                      get_cata_dict(config))
        if config.getboolean(section, 'save'):
            pass
            #a model.save(sc, os.path.join(args.model_path + section))
        evaluation(section, model, test_data, args.verbose, output=args.output + section)
    os.system('rm ' + args.file_buffer)
