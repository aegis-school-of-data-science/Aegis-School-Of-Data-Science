# -*- coding: utf-8 -*-
"""
@author: Sudarshan John, arien74@gmail.com
"""

#import numpy as np
import pandas as pd
from Constants import *

from matplotlib import pyplot as plt
from sklearn.grid_search import GridSearchCV
from sklearn.neighbors import KNeighborsClassifier

choice = int(raw_input("\nFor Training enter 1\nFor Prediction enter 2\nYour choice..."))

if choice == 1:
    # Read the pre-processed file
    data = pd.read_csv(pre_processed_file)
    
    # Select the features and classification
    X = data.iloc[:,4:182]
    y = data['Result']
    
    # KNN
    # Try with up to 50 neighbours
    k_range = range(1,51)
    weight_options = ['uniform','distance']
    param_grid = dict(n_neighbors=k_range, weights=weight_options)
     
    knn = KNeighborsClassifier()
    
    # Check with GridSearchCV
    grid = GridSearchCV(knn, param_grid, cv=5, scoring='accuracy')
    grid.fit(X, y)
     
    grid_mean_scores_1 = [result.mean_validation_score for result in grid.grid_scores_ if result[0]['weights'] == 'uniform' ]
    grid_mean_scores_2 = [result.mean_validation_score for result in grid.grid_scores_ if result[0]['weights'] == 'distance' ]
     
    print grid.best_score_
    print grid.best_params_
    print grid.best_estimator_
     
    plt.plot(k_range, grid_mean_scores_1)
    plt.plot(k_range, grid_mean_scores_2)
    plt.xlabel('Value of K for KNN')
    plt.ylabel('Cross Validate Score')
    plt.title("KNN using uniform/distance options for weight")
    plt.savefig('knn_multi-weights.png')
    
    knn = KNeighborsClassifier(n_neighbors=20, weights='distance')
    knn.fit(X, y)
    
    import pickle
    pickle.dump(knn, open(knn_pickle_file, "w"))

else:

    import pickle
    
    try:
        clf = pickle.load(open(knn_pickle_file, "r"))
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
