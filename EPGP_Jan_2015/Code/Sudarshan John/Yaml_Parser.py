# -*- coding: utf-8 -*-
"""
@author: Sudarshan John, arien74@gmail.com
"""

import yaml
import os
#import logging

from Constants import *

def AddUniqueToList(strList, val):
    if val not in strList:
        strList.append(val)
    
def ProcessYaml(filename):
    print filename

    with open(filename, 'r') as f_in:
        doc = yaml.load(f_in)
        f_in.close()

    match_type = doc['info']['match_type']

    # Filter out match types
    if match_type not in match_types:
        return -1

    if 'winner' in doc['info']['outcome']:
        outcome_winner = doc['info']['outcome']['winner']
    elif 'result' in doc['info']['outcome']:
        outcome_winner = doc['info']['outcome']['result']
    
    if outcome_winner in ['tie','no result']:
        return -1
        
    team_1 = doc['info']['teams'][0]
    team_2 = doc['info']['teams'][1]

    # Restrict to specific teams only
    if ((team_1 not in teams_list) or (team_2 not in teams_list)):
        return -1
    
    # Omit tie and no-result
    
    if 'city' in doc['info']:
        city = doc['info']['city']
    else:
        city = "NONE"

    # Change for other match types and conditions
    dates = doc['info']['dates']

    toss_decision = doc['info']['toss']['decision']
    toss_winner = doc['info']['toss']['winner']

    tmp_venue = doc['info']['venue']
    venue = tmp_venue.replace(",","-")

    if venue not in venue_map:
        venue_map['counter'] = venue_map['counter'] + 1
        venue_map[venue] = venue_map['counter']

#    team_venue_map[team_1][venue] = venue_map[venue]
#    team_venue_map[team_2][venue] = venue_map[venue]
    
    t1_players_list = []
    t2_players_list = []
    
    innings = doc['innings']
    for inning in innings:
        deliveries = inning.values()[0]['deliveries']
        team_name = inning.values()[0]['team']

        t_flag = -1
        
        if team_name == team_1:
            t_flag = 1
        else:
            t_flag = 2

        for delivery in deliveries:
            ball_dic = delivery.values()[0]
            player_name = ""
            
            if 'batsman' in ball_dic:
                player_name = ball_dic['batsman']
                if player_name not in players_map[team_name]:
                    players_map[team_name][player_name] = players_map[team_name]['counter'] + 1
                    players_map[team_name]['counter'] += 1
                
                if t_flag == 1:
                    AddUniqueToList(t1_players_list, player_name)
                else:
                    AddUniqueToList(t2_players_list, player_name)
                    
            if 'non_striker' in ball_dic:
                player_name = ball_dic['non_striker']
                if player_name not in players_map[team_name]:
                    players_map[team_name][player_name] = players_map[team_name]['counter'] + 1
                    players_map[team_name]['counter'] += 1
                
                if t_flag == 1:
                    AddUniqueToList(t1_players_list, player_name)
                else:
                    AddUniqueToList(t2_players_list, player_name)
            
            if 'bowler' in ball_dic:
                player_name = ball_dic['bowler']
                
                if t_flag == 1:
                    tmp = team_2
                    AddUniqueToList(t2_players_list, player_name)
                else:
                    tmp = team_1
                    AddUniqueToList(t1_players_list, player_name)
                    
                if player_name not in players_map[tmp]:
                    players_map[tmp][player_name] = players_map[tmp]['counter'] + 1
                    players_map[tmp]['counter'] += 1
            
    # Use , as the field seperator
    line = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (filename, 
         team_1, team_2, match_type, dates[0].strftime('%Y-%m-%d'), 
         venue, city, toss_winner, toss_decision, outcome_winner)

    # Pad the team list with strings 
    if len(t1_players_list) < MAX_TEAM_MEMBERS:
        for i in range(MAX_TEAM_MEMBERS-len(t1_players_list)):
            t1_players_list.append("AAA")

    for s in t1_players_list:
        line = line + "," + s
    
    if len(t2_players_list) < MAX_TEAM_MEMBERS:
        for i in range(MAX_TEAM_MEMBERS-len(t2_players_list)):
            t2_players_list.append("BBB")

    for s in t2_players_list:
        line = line + "," + s

    line = line + "\n"
    
    f_out.write(line)
    
    #
    # Flip the teams around so that the opposite part is also part of the training
    #
 
    line = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (filename, 
         team_2, team_1, match_type, dates[0].strftime('%Y-%m-%d'), 
         venue, city, toss_winner, toss_decision, outcome_winner)

    # Pad the team list with strings 
    if len(t2_players_list) < MAX_TEAM_MEMBERS:
        for i in range(MAX_TEAM_MEMBERS-len(t2_players_list)):
            t2_players_list.append("AAA")

    for s in t2_players_list:
        line = line + "," + s

    if len(t1_players_list) < MAX_TEAM_MEMBERS:
        for i in range(MAX_TEAM_MEMBERS-len(t1_players_list)):
            t1_players_list.append("BBB")

    for s in t1_players_list:
        line = line + "," + s

    line = line + "\n"
    
    f_out.write(line)   
    
    return 0

f_out = open(parsed_yaml_file, "w")

hd_list = ["TA-", "TB-"]
for s in hd_list:
    for i in range(MAX_TEAM_MEMBERS):
        file_header = file_header + "," + s + str(i+1)

file_header = file_header + "\n"
    
f_out.write(file_header)

processed_files = ""

ret_cd = -1

cnt = 0

for dirname, dirnames, filenames in os.walk('..\Cricsheet1'):
    for tmp_str in filenames:
        if ".yaml" in tmp_str:
            # Check whether pre-processing of the file already done
            if tmp_str in processed_files:
                print "%s already processed, skipping ..." % (tmp_str)
            else:
                try:
                    filename = os.path.join(dirname, tmp_str)
                    ret_cd = ProcessYaml(filename)

                    cnt += 1
                    
                    if (ret_cd != 0):
                        print "Skipped...", tmp_str
                    else:
                        print "processed...", tmp_str
                except:
                    print ("*** failed *** > " + tmp_str)

import json

try:
    with open(players_info_file, "w") as f:
        json.dump(players_map, f)
    f.close()
except:
    print "File error ...", players_info_file

try:
    with open(venue_info_file, "w") as f:
        json.dump(venue_map, f)
    f.close()
except:
    print "File error ...", venue_info_file
    
f_out.close()
