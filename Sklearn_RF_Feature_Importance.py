# -*- coding: utf-8 -*-



import sys
print (sys.path)
import os
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
 
import dask.dataframe as dd
from dask.distributed import Client , LocalCluster



# Load data from csv file in  X_file_path  and return pandas dataframe
def load_X_data(X_file_path):
    
    try:
        # Load data from the CSV file which include the header as pandas dataframe
        df = pd.read_csv(X_file_path, sep=',')              
        print ( "X dataframe shape ", df.shape)
        # print ( "X dataframe " , df)
    
        df = df.iloc [:, :100]  
        print ( "X sliced dataframe shape ", df.shape)
        print ( "X sliced ", df) 
        
        df = dd.from_pandas(df , npartitions = 2 )
        print ( "df   dask   ", df)
        
        return df

    except Exception as e:
        print("Error occurred while loading descriptors CSV data:", e)
        
        
        
# Load data from csv file in  y_file_path and return pandas dataframe    
def load_y_data(y_file_path):
    
    try:
       
        # Load data from the CSV file which include the header return pandas dataframe  
        df = dd.read_csv(y_file_path, sep= ','    )
        print ( "df dataframe shape ", df.shape)
        
        # exclude the first column and first row
        df = df.iloc[:, 1:]    
        print ( "y dataframe shape", df.shape)
        print ( "y dataframe", df)
        
        return  df

    except Exception as e:
        print("Error occurred while loading concentrations CSV data:", e)
        
       
        
def feature_importacne(X_file_path, y_file_path, num_feature_to_select):
    
    def feature_importance(X, y , num_feature_to_select):
        
        rf_model = RandomForestRegressor(n_estimators = 10)

        
        rf_model.fit(X, y)
        
        # Get feature importances
        feature_importances = rf_model.feature_importances_
        print("feature_importances type:", type(feature_importances))
        print("feature_importances includes  :" , feature_importances)
        
        # Sort feature importances in descending order
        sorted_indices = np.argsort(feature_importances)[::-1]
        print("sorted_indices type:", type(sorted_indices))
        print("sorted_indices includes  :" , sorted_indices)
        
        # Choose the top k features
        top_k_features = sorted_indices[:num_feature_to_select]
        print("top_k_features type:", type(top_k_features))
        print("top_k_features includes  :" , top_k_features)
         
       
       # Get the names of the selected features
        selected_feature_names = X.columns[top_k_features]
        print("selected_feature_names type :" , type(selected_feature_names))
       
       # Retrieve the selected features from the input with their column names
        selected_features = X[selected_feature_names]
        print("selected_features type :" , type(selected_features))       
        print("Selected Feature Names:", selected_feature_names)
        print("Selected Features Dataframe:", selected_features)       
        print ("selected columns " , selected_features.columns)
        
        
        return selected_features
    
        
    X = load_X_data(X_file_path)
    y = load_y_data(y_file_path)
    
    y = y['algae67kpa']
    print("y type:", type(y))
    print("y shape:", y.shape)
    print("y contain:", y)
    
    X = X.astype('float32') # Features MUST be float32
    y = y.astype('float32')  # Labels MUST be float32
     
    selected = feature_importance ( X, y , num_feature_to_select )    
    print ("selected  type: ", type(selected))  
    print ("selected : ", selected.shape)
     
    
    selected = pd.DataFrame(selected)
    print ("selected  final type: ", type(selected))  
    print ("selected  final: ", selected.shape)
 
       
    return selected



# Function gets the pandas dataframe and write it to csv and returns file path dictionaty
def write_to_csv(X_selected, output_path):
    
        
    # Create a separate directory for the output file
    try:
        
      # Create the output directory if it doesn't exist                                                        
       os.makedirs(output_path, exist_ok = True)     
       file_name = 'selected_sklearn_rf_dask.csv'
       file_path = os.path.join(output_path, file_name)
                
       X_selected.to_csv(file_path, sep = ',', header = True, index = True ) 

       file_path_dict = {'sklearn_rf_dask': file_path}
       print("CSV file written successfully.")
       print ("CSV file size is  " , os.path.getsize(file_path))
       print ("CSV file column number is  " , X_selected.shape[1])
       print ("file_path_dictionary is  " , file_path_dict)
       return file_path 
   
    except Exception as e:
       print("Error occurred while writing matrices to CSV:", e)
       
       
if __name__ == '__main__': 
             
    
    # Create Lucalluster with specification for each dask worker to create dask scheduler     
    cluster = LocalCluster ()    
    
    #Create the Client using the cluster
    client = Client(cluster) 

       
    X_file_path = r'/mmfs1/projects/bakhtiyor.rasulev/Rahil/a67kpa_filtered/combinatorial.csv' 
    y_file_path = r'/mmfs1/projects/bakhtiyor.rasulev/Rahil/a67kpa_filtered/endpoint_a67kpa.csv'  
    output_path = r'a67kpa_filtered' 
    
    num_feature_to_select = 1000
    
    X_selected = feature_importacne ( X_file_path, y_file_path , num_feature_to_select)
    
    file_path = write_to_csv(X_selected, output_path)
    print (file_path)
 
    scheduler_address = client.scheduler.address
    print("Scheduler Address:", scheduler_address)
    print(cluster.workers)
    print(cluster.scheduler)
    print(cluster.dashboard_link)
    print(cluster.status)
   
    client.close()
    cluster.close()
    
    

