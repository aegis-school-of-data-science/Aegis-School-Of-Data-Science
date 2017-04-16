# -*- coding: utf-8 -*-
"""
@author: Sudarshan John, arien74@gmail.com
"""
#

from Constants import *
import pandas as pd
import numpy as np
import sqlite3
import json

def get_max_team_size():
    max_t_size = 0

    for k, v in players_map.iteritems():
        if v['counter'] > max_t_size:
            max_t_size = v['counter']
    return max_t_size

try:
    with open(players_info_file, "r") as f:
        players_map = json.load(f)
    f.close()
except:
    print "File error ...", players_info_file

try:
    with open(venue_info_file, "r") as f:
        venue_map = json.load(f)
    f.close()
except:
    print "File error ...", venue_info_file

# Load all info into data strutures in memory
conn = sqlite3.connect("Capstone.db")
df_teams = pd.read_sql_query("SELECT * from teams", conn)
#conn.commit()
df_venues = pd.read_sql_query("SELECT * from venues", conn)
conn.close()

print "\n\t\tSelect Match Type (select number)..."
print "\t0 - One Day International\n\t1 - T20 International"

choice_matchtype = int(raw_input("\n\tYour choice ..."))

print "\n\t\tSelect Teams from the below"

print df_teams.to_string(index = False)
#print(df.to_csv(sep='\t', index=False))
#print(df.to_csv(columns=['A', 'B', 'C'], sep='\t', index=False))

choice_t1 = int(raw_input("\nChoose Team 1 (select number)..."))
choice_t2 = int(raw_input("Choose Team 2 (select number)..."))

team1 = df_teams.ix[df_teams.Id == choice_t1, 'Name'].iloc[0]
team2 = df_teams.ix[df_teams.Id == choice_t2, 'Name'].iloc[0]

print "\nMatch -> %s  Vs  %s" % (team1, team2)

max_team_size = get_max_team_size()


import pickle

try:
    clf = pickle.load(open(logreg_pickle_file, "r"))
except:
    print "--- Error loading pickle file ---"

team_vector = []
# index 4 onwards, match type, team a, 87 players, team b, 87 players
for i in range(1+1+max_team_size+1+max_team_size+1):
    team_vector.append(0)

team_vector[0] = choice_matchtype
team_vector[1] = choice_t1
team_vector[1+1+max_team_size] = choice_t2

import random
#from random import randint

t1_max = players_map[team1]['counter']
t2_max = players_map[team2]['counter']


print "\nChoose Match Location"
print "\nChoose 1 for %s\nChoose 2 for %s" % (team1, team2)
choice_location = int(raw_input("\n\tYour choice ... "))

match_loc = 1

if choice_location == 1:
    df_t1 = df_venues[df_venues['Team'] == team1]
    t = df_t1.sample(1)
    match_loc = t['Id'].iloc[0]
    print "\nGround selected ...", t['Name'].iloc[0]
else:
    df_t2 = df_venues[df_venues['Team'] == team2]
    t = df_t2.sample(1)
    match_loc = t['Id'].iloc[0]
    print "\nGround selected ...", t['Name'].iloc[0]

raw_input("\nPress Enter to continue...")
  
print "\nTeam -> ", team1
print ""

#t1_players = [randint(1,t1_max-1) for p in range(1,12)]
t1_players = random.sample(range(t1_max-1), 11)
for p in t1_players:
    team_vector[p+1] = 1
    for k1, v1 in players_map[team1].iteritems():
        if v1 == p:
            print k1

print "\nTeam -> ", team2
print ""

#t2_players = [randint(1,t2_max-1) for p in range(1,12)]
t2_players = random.sample(range(t2_max-1), 11)
for p in t2_players:
    team_vector[1+max_team_size+1+p] = 1
    for k1, v1 in players_map[team2].iteritems():
        if v1 == p:
            print k1

team_vector[1+1+max_team_size+1+max_team_size] = match_loc

team_vector = np.array(team_vector).reshape(1, (len(team_vector)))
#team_vector = scaler.transform(team_vector)

raw_input("\nPress Enter to see result...")

outcome = clf.predict(team_vector)

if outcome == 0:
    print "\n==== WINNER -> %s ====" % (team2)
else:
    print "\n==== WINNER -> %s ====" % (team1)
