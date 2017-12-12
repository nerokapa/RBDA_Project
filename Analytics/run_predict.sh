#!bash

python predict_model.py -config model_config.cfg -file_buffer roam \
                       -o res/onlyEnAllFeatures_more_than_5m.dat -model res/model
