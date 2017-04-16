# -*- coding: utf-8 -*-
"""
@author: Sudarshan John, arien74@gmail.com
"""

parsed_yaml_file = "parsed-yaml.csv"
input_train_file = "input-file.csv"

teams_list = ["Australia","Bangladesh","England","India","New Zealand","Pakistan","Sri Lanka","South Africa","West Indies","Zimbabwe"]

match_types = ["ODI","ODM","T20","IT20"]

team_map = {"Australia":1,"Bangladesh":2,"England":3,"India":4,"New Zealand":5,"Pakistan":6,"Sri Lanka":7,"South Africa":8,"West Indies":9,"Zimbabwe":10}

match_type_map = {"ODI":0,"ODM":0,"T20":1,"IT20":1}

toss_decision_map = {"bat":0,"field":1}

file_header ="File,Team-A,Team-B,Match-Type,Date,Venue,City,Toss-Winner,Decision,Winner"

players_map = {"Australia": {"counter":0},
               "Bangladesh": {"counter":0},
               "England": {"counter":0},
               "India": {"counter":0},
               "New Zealand": {"counter":0},
               "Pakistan": {"counter":0},
               "Sri Lanka": {"counter":0},
               "South Africa": {"counter":0},
               "West Indies": {"counter":0},
               "Zimbabwe": {"counter":0}
               }

players_info_file = "players.json"

pre_processed_file = 'out.csv'

knn_pickle_file = 'knn_model_pickle'
logreg_pickle_file = 'logreg_model_pickle'
decisiontree_pickle_file = 'decisiontree_model_pickle'
randomforest_pickle_file = 'randomforest_model_pickle'

MAX_TEAM_MEMBERS = 13

venue_map = {"counter":0}

team_venue_map = {"Australia": {"counter":0},
               "Bangladesh": {"counter":0},
               "England": {"counter":0},
               "India": {"counter":0},
               "New Zealand": {"counter":0},
               "Pakistan": {"counter":0},
               "Sri Lanka": {"counter":0},
               "South Africa": {"counter":0},
               "West Indies": {"counter":0},
               "Zimbabwe": {"counter":0}
               }

venue_info_file = "venue.json"
