# mahini-Random_Forest_Feature_Importance_sklearn
This Python code computes the feature selection by using feature_importances_  function in RandomForestRegressor in sklearn.ensemble. 
It takes two csv files of features (X) and endpoint (y) and read them to dask dataframes.
Next, create the RandomForestRegressor model and fit the whole dataset.
Next, calculate the feature_importances_ 
Then, Sort the features based on their feature_importances_
And finally returns the csv file containing the k top features with higher feature importances.


