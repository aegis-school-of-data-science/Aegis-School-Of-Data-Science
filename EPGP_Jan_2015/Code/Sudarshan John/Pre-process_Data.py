# -*- coding: utf-8 -*-
"""
Created on Fri Feb 12 14:19:54 2016

@author: Sudarshan John, arien74@gmail.com
"""

import pandas as pd
import json

from Constants import *

def get_max_team_size():
    max_t_size = 0

    for k, v in players_map.iteritems():
        if v['counter'] > max_t_size:
            max_t_size = v['counter']
    return max_t_size


data = pd.read_csv(parsed_yaml_file, sep=',',parse_dates=['Date'])

try:
    with open(players_info_file, "r") as f:
        players_map = json.load(f)
    f.close()
except:
    print "File error"

try:
    with open(venue_info_file, "r") as f1:
        venue_map = json.load(f1)
    f1.close()
except:
    print "File error"

max_team_size = get_max_team_size()
print max_team_size

start_date = data[:1]['Date']
end_date = data[-1:]['Date']
print start_date.values[0]
print end_date.values[0]

df = pd.DataFrame()
df['Result'] = (data['Team-A'] == data['Winner']).astype(int)
df['Toss-Winner'] = (data['Team-A']==data['Toss-Winner']).astype(int)

hd_list = ["TA-", "TB-"]
tm_col_list = ["Team-A","Team-B"]

for index, row in data.iterrows():

    print "Processing row ...", index
    
    df.ix[index, 'Toss-Winner'] = team_map[data.ix[index, 'Toss-Winner']]
    df.ix[index, 'Decision'] = toss_decision_map[data.ix[index, 'Decision']]
    df.ix[index, 'Match-Type'] = match_type_map[data.ix[index, 'Match-Type']]

    
    for (t1, t2) in zip(tm_col_list,hd_list):
        team_name = data.ix[index, t1]

        df.ix[index, t1] = team_map[team_name]    
        
        col_name = ""
        for i in range(max_team_size):
            col_name = t2 + str(i+1)
            df.ix[index, col_name] = 0
            
        for j in range(MAX_TEAM_MEMBERS):
            col_name = t2 + str(j+1)
            player = data.ix[index, col_name]
            
            if player not in ['AAA','BBB']:
                player_index = players_map[team_name][player]
                tmp_name = t2 + str(player_index)
                df.ix[index, tmp_name] = 1

    df.ix[index, 'Venue'] = venue_map[data.ix[index, 'Venue']]
    
df.to_csv(pre_processed_file)
