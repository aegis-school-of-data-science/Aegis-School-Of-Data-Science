# -*- coding: utf-8 -*-
"""
@author: Sudarshan John, arien74@gmail.com
"""

import numpy as np
import pandas as pd
from Constants import *
import pickle
from matplotlib import pyplot as plt
from sklearn.grid_search import GridSearchCV
from sklearn.linear_model import LogisticRegression

choice = int(raw_input("\nFor Training enter 1\nFor Prediction enter 2\nYour choice..."))

if choice == 1:
    # Read the pre-processed file
    data = pd.read_csv(pre_processed_file)
    
    # Select the features and classification
    X = data.iloc[:,4:182]
    y = data['Result']
    
    # Logistic Regression
    param_grid = {'C': [0.001, 0.01, 0.1, 1, 10, 100, 1000] }
      
    logreg = LogisticRegression(penalty='l2')
    grid = GridSearchCV(logreg, param_grid, cv=5, scoring='accuracy')
    grid.fit(X, y)
      
    grid_mean_scores_1 = [result.mean_validation_score for result in grid.grid_scores_]
      
    print grid.best_score_
    print grid.best_params_
    print grid.best_estimator_
      
    plt.plot(param_grid['C'], grid_mean_scores_1)
    plt.title("Logistic Regression scores")
    plt.savefig('logreg_multi-Cs.png')
    
    logreg = LogisticRegression(C=100)
    logreg.fit(X, y)
    
    pickle.dump(logreg, open(logreg_pickle_file, "w"))
    
else:
    
    try:
        clf = pickle.load(open(logreg_pickle_file, "r"))
    except:
        print "--- Error loading pickle file ---"
    
    team_vector = []
    # index 4 onwards, match type, team a, 87 players, team b, 87 players
    for i in range(1+1+87+1+87+1):
        team_vector.append(0)
    
    team_vector[0] = 0                # Match type
    team_vector[1] = 2                # Team 1
    team_vector[1+1+87] = 1           # Team 2
    team_vector[1+1+87+1+87] = 1      # Venue

    print clf.predict(team_vector)
