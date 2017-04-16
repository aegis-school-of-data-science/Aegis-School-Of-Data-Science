# -*- coding: utf-8 -*-
"""
Created on Fri Jun 17 22:30:00 2016

@author: Swapnali
"""
import pandas as pd
airport_lookupDF = pd.read_csv('F:/BA_BD/capstone project/Airline Data/DataSource/Airport_Lookup.csv',header = 0)
airport_lookupDF.head()
trainDF = pd.read_csv('C:/Users/Swapnali/Documents/capstone/FinalFlightsNumeric.csv', header = 0) 
carrierDF = trainDF[['UNIQUE_CARRIER', 'CARRIER_CODE']].drop_duplicates() 
trainDF.drop('UNIQUE_CARRIER', axis = 1, inplace = True)
hdaysDF = trainDF[['MONTH', 'DAY_OF_MONTH', 'HDAYS']].drop_duplicates()
scalingDF = trainDF[['DISTANCE', 'HDAYS']].astype('float') 
categDF = trainDF[['MONTH', 'DAY_OF_MONTH', 'ORIGIN_AIRPORT_ID', 
                    'DEST_AIRPORT_ID', 'ARR_HOUR', 'DEP_HOUR', 
                    'CARRIER_CODE', 'DAY_OF_WEEK']]
from sklearn.preprocessing import OneHotEncoder
encoder = OneHotEncoder() 
categDF_encoded = encoder.fit_transform(categDF)
type(categDF_encoded)
from scipy import sparse 
scalingDF_sparse = sparse.csr_matrix(scalingDF)
x_final = sparse.hstack((scalingDF_sparse, categDF_encoded))
y_final = trainDF['ARR_DELAY'].values
from sklearn.cross_validation import train_test_split
x_train, x_test, y_train, y_test = train_test_split(x_final,y_final,test_size = 0.2,random_state = 0) 
x_train_numerical = x_train[:, 0:2].toarray()
x_test_numerical = x_test[:, 0:2].toarray()
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler() 
scaler.fit(x_train_numerical) 
x_train_numerical = sparse.csr_matrix(scaler.transform(x_train_numerical)) 
x_test_numerical = sparse.csr_matrix(scaler.transform(x_test_numerical))
x_train[:, 0:2] = x_train_numerical
x_test[:, 0:2] = x_test_numerical
from sklearn.linear_model import SGDRegressor
from sklearn.grid_search import GridSearchCV
import numpy as np
SGD_params = {'alpha': 10.0**-np.arange(1,7)} 
SGD_model = GridSearchCV(SGDRegressor(random_state = 0), SGD_params, scoring = 'mean_absolute_error', cv = 5) 
SGD_model.fit(x_train, y_train) 
param_grid={'alpha':[  1.00000e-01,   1.00000e-02,   1.00000e-03,   1.00000e-04,1.00000e-05,   1.00000e-06]}
GridSearchCV(cv=5,estimator=SGDRegressor(alpha=0.0001, epsilon=0.1, eta0=0.01, fit_intercept=True,l1_ratio=0.15, learning_rate='invscaling', loss='squared_loss',n_iter=5, penalty='l2', power_t=0.25, random_state=0, shuffle=False,verbose=0, warm_start=False),fit_params={}, iid=True, loss_func=None, n_jobs=1,param_grid={'alpha':[  1.00000e-01,   1.00000e-02,   1.00000e-03,   1.00000e-04,1.00000e-05,   1.00000e-06]},pre_dispatch='2*n_jobs', refit=True, score_func=None,scoring='mean_absolute_error', verbose=0)
from sklearn.metrics import mean_absolute_error
y_true, y_pred = y_test, SGD_model.predict(x_test)
print 'Mean absolute error of SGD regression was:'
print(mean_absolute_error(y_true, y_pred))
def max_num_flights(codes):
    
    num_store = np.zeros(len(codes)) 
    
    if len(codes) < 1:
        print('Try entering your city/airport again. No matching airports found.') 
        return                                                                    
    
    for i in xrange(len(codes)):
        num_flights = trainDF.MONTH[trainDF.ORIGIN_AIRPORT_ID == codes[i]].count() 
        num_store[i] = num_flights                                                
           
        
   
        
    max_ind = int(np.where(num_store == max(num_store))[0])
    
    return(codes[max_ind])                                       
def delay_prediction(origin = 'Fort Worth', destination = 'Chicago', carrier = 'American', 
                    dept_time = 17, arr_time = 19, month = 5, day = 15, weekday = 'Wednesday'):
                        
    
    
         
   
         
    carrier_dict = {'Endeavor':1, 'American':2, 'Alaska':3, 'JetBlue':4, 'Delta':5,
                    'ExpressJet':6, 'Frontier':7, 'AirTran':8, 'Hawaiian':9, 'Envoy':10,
                    'SkyWest':11, 'United':12, 'US Airways':13, 'Virgin':14,
                    'Southwest':15, 'Mesa':16}
                         
   
         
    weekday_dict = {'Monday':1, 'Tuesday':2, 'Wednesday':3, 'Thursday':4,
                    'Friday':5, 'Saturday':6, 'Sunday':7}
                         
    
         
    origin_codes = list(airport_lookupDF[airport_lookupDF.Description.str.contains(origin)].Code)
    destination_codes = list(airport_lookupDF[airport_lookupDF.Description.str.contains(destination)].Code)
         
    
                                                                                                                                                                                                                                                                                                                                         
    origin_code = max_num_flights(origin_codes)
    destination_code = max_num_flights(destination_codes)
         
   
         
    hdays = np.array(float(hdaysDF[(hdaysDF.MONTH == month) & (hdaysDF.DAY_OF_MONTH == day)].HDAYS))
         
   
       
    try:
        distance = np.array(float(trainDF[(trainDF.ORIGIN_AIRPORT_ID == origin_code) & 
                    (trainDF.DEST_AIRPORT_ID == destination_code)].DISTANCE.drop_duplicates()))
    except:
        print 'Route was not found in the data. Please try a different nearby city or a new route.'
        return
         
    carrier_num = carrier_dict[carrier]
    weekday_num = weekday_dict[weekday]
         
  
         
    numerical_values = np.c_[distance, hdays]
         
   
         
    numerical_values_scaled = scaler.transform(numerical_values)
        
    
         
    categorical_values = np.zeros(8)
    categorical_values[0] = int(month)
    categorical_values[1] = int(day)
    categorical_values[2] = int(origin_code)
    categorical_values[3] = int(destination_code)
    categorical_values[4] = int(arr_time)
    categorical_values[5] = int(dept_time)
    categorical_values[6] = int(carrier_num)
    categorical_values[7] = int(weekday_num)
         
    
         
    categorical_values_encoded = encoder.transform([categorical_values]).toarray()
         
   
         
    final_test_example = np.c_[numerical_values_scaled, categorical_values_encoded]
         
   
         
    pred_delay = SGD_model.predict(final_test_example)
    print 'Your predicted delay is', int(pred_delay[0]), 'minutes.'
    return

    delay_prediction(origin = 'Washington, DC', destination = 'Miami, FL', 
                carrier = 'United', dept_time = 17, arr_time = 21,
                month = 3, day = 19, weekday = 'Thursday')
                
    delay_prediction(origin = 'Dallas', destination = 'Chicago', 
                carrier = 'American', dept_time = 17, arr_time = 20,
                month = 1, day = 28, weekday = 'Wednesday')

