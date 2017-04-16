# -*- coding: utf-8 -*-
"""
@author: Sudarshan John, arien74@gmail.com
"""

from Constants import *
import pandas as pd
import sqlite3
import json

choice = int(raw_input("\nFor team setup to DB enter 1\nFor players setup into DB enter 2\nFor venue setup into DB enter 3\nYour choice..."))

if choice == 1:

    conn = sqlite3.connect("Capstone.db")
    #df = pd.read_sql_query("SELECT * from surveys", con)
    #df = pd.DataFrame.from_dict(team_map, 'index')
    #pd.DataFrame.to_sql('Teams', con)
    
    for k, v in team_map.iteritems():
        try:
            conn.execute("INSERT INTO TEAMS (ID, NAME) VALUES (?, ?)", (v, k))
        except:
            print "Values already present in TEAMS...", v

    conn.commit()
    conn.close()

elif choice == 2:
    
    try:
        with open(players_info_file, "r") as f:
            players_map = json.load(f)
        f.close()
    except:
        print "File error"

    conn = sqlite3.connect("Capstone.db")
    
    for k, v in players_map.iteritems():
        for k1, v1 in v.iteritems():
            try:
                if k1 != 'counter':
                    print "Inserting %s %s %d" % (k, k1, v1)
                    conn.execute("INSERT INTO PLAYERS (TEAMNAME, NAME, ID) VALUES (?, ?, ?)", (k, k1, v1))
            except:
                print "Values already present ...", k1

    conn.commit()
    conn.close()
    
elif choice == 3:
    
    try:
        with open(venue_info_file, "r") as f:
            venue_map = json.load(f)
        f.close()
    except:
        print "File error"

    conn = sqlite3.connect("Capstone.db")
    
    for k, v in venue_map.iteritems():
        try:
            t = "Australia"
            if k != 'counter':
                print "Inserting %s %d" % (k, v)
                conn.execute("INSERT INTO VENUES (TEAM, NAME, ID) VALUES (?, ?, ?)", (t, k, v))
        except:
            print "Values already present ...", k

    conn.commit()
    conn.close()