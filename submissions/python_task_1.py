import pandas as pd
from datetime import datetime, timedelta
##1. 

def generate_car_matrix(df)->pd.DataFrame:
    """
    Creates a DataFrame  for id combinations.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Matrix generated with 'car' values, 
                          where 'id_1' and 'id_2' are used as indices and columns respectively.
    """
    # Write your logic here
    matrix = df.pivot(index='id_1', columns='id_2', values='car')
    matrix.fillna(0.0,inplace=True)

    return matrix

##2.
def get_type_count(df)->dict:
    """
    Categorizes 'car' values into types and returns a dictionary of counts.

    Args:
        df (pandas.DataFrame)

    Returns:
        dict: A dictionary with car types as keys and their counts as values.
    """
    # Write your logic here
    df['car_type'] = df['car'].apply(lambda x: 'low' if x <= 15 else ('medium' if x <= 25 else 'high'))
    x = df.groupby("car_type").size().to_dict()

    return dict(sorted(x.items()))

## 3.
def get_bus_indexes(df)->list:
    """
    Returns the indexes where the 'bus' values are greater than twice the mean.

    Args:
        df (pandas.DataFrame)

    Returns:
        list: List of indexes where 'bus' values exceed twice the mean.
    """
    # Write your logic here
    mean_bus = df['bus'].mean()

    bus_indexe = df[df['bus'] > 2 * mean_bus].index.sort_values(ascending=True)

    return list(bus_indexe)

##4.
def filter_routes(df)->list:
    """
    Filters and returns routes with average 'truck' values greater than 7.

    Args:
        df (pandas.DataFrame)

    Returns:
        list: List of route names with average 'truck' values greater than 7.
    """
    # Write your logic here
    truck_avg = df.groupby("route")["truck"].mean()
    filter_route = truck_avg[truck_avg >7].index.sort_values()
    return list(filter_route)

##5.
def multiply_matrix(matrix)->pd.DataFrame:
    """
    Multiplies matrix values with custom conditions.

    Args:
        matrix (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Modified matrix with values multiplied based on custom conditions.
    """
    # Write your logic here
    new = matrix.copy()     # so changes doesn't effect input matrix
    
    new[matrix > 20] = (new[matrix > 20]*0.75).round(1)
    new[matrix <=20] = (new[matrix <=20]*1.25).round(1)

    return new

##6.

def time_check(df):
    """
    Verifies the completeness of data by checking timestamps for each unique (`id`, `id_2`) pair.

    Args:
        df (pandas.DataFrame): Input DataFrame from datasets2.csv

    Returns:
        pd.Series: Boolean series indicating if each (`id`, `id_2`) pair has incorrect timestamps.
    """
    df['start_time'] = pd.to_datetime(df['startDay'] + ' ' + df['startTime'], format='%A %H:%M:%S')
    df['end_time'] = pd.to_datetime(df['endDay'] + ' ' + df['endTime'], format='%A %H:%M:%S')
    df['time_duration'] = df['end_time'] - df['start_time']
    time_periods = df.groupby(['id', 'id_2'])['time_duration'].agg(['min', 'max'])
    
    full_week_duration = pd.Timedelta(days=7)
    incomplete_time = (time_periods['max'] - time_periods['min'] < full_week_duration)
    
    return incomplete_time




data1 = r'datasets/dataset-1.csv'
data2 = r'datasets/dataset-2.csv'

car_matrix = generate_car_matrix(pd.read_csv(data1))            
car_type_counts = get_type_count(pd.read_csv(data1))            
bus_index = get_bus_indexes(pd.read_csv(data1))                 
filtered_truck_routes = filter_routes(pd.read_csv(data1))       
multiplied_matrix = multiply_matrix(car_matrix)                 
completeness_check = time_check(pd.read_csv(data2))      


# print("time_check : \n", completeness_check)
