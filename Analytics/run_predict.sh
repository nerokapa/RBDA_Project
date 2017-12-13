#!bash

python predict_model.py -config model_config.cfg -file_buffer roam -strip \
                       -o res/UsingTwitter.dat -model res/model -twitter ../Data/twitter_data.json
