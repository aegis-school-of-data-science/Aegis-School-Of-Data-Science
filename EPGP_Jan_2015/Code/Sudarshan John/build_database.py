# -*- coding: utf-8 -*-
"""
@author: Sudarshan John, arien74@gmail.com
"""

from Constants import *
import pandas as pd
import sqlite3
import json

conn = sqlite3.connect("Capstone.db")
#df = pd.read_sql_query("SELECT * from surveys", con)
#df = pd.DataFrame.from_dict(team_map, 'index')
#pd.DataFrame.to_sql('Teams', con)

for k, v in team_map.iteritems():
    try:
        conn.execute("INSERT INTO TEAMS (ID, NAME) VALUES (?, ?)", (v, k))
    except:
        print "Values already present ...", v

try:
    with open(players_info_file, "r") as f:
        players_map = json.load(f)
    f.close()
except:
    print "File error"

for k, v in players_map.iteritems():
    for k1, v1 in v.iteritems():
        try:
            if k1 != 'counter':
                print "Inserting %s %s %d" % (k, k1, v1)
                conn.execute("INSERT INTO PLAYERS (TEAMNAME, NAME, ID) VALUES (?, ?, ?)", (k, k1, v1))
        except:
            print "Values already present ...", v
conn.commit()
conn.close()