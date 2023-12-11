import pandas as pd
from datetime import datetime, timedelta,time

df3 = pd.read_csv("datasets/dataset-3.csv")


'''task2. q1'''
## 1.
def calculate_distance_matrix(df)->pd.DataFrame():
    """
    Calculate a distance matrix based on the dataframe, df.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Distance matrix
    """
    # Write your logic here
    # Create a DataFrame with unique IDs
    data = df
    unique_ids = sorted(set(data['id_start'].unique()) | set(data['id_end'].unique()))
    distance_matrix = pd.DataFrame(index=unique_ids, columns=unique_ids)

    # Set diagonal values to 0
    distance_matrix = distance_matrix.fillna(0)

    # Populate the distance matrix
    for _, row in data.iterrows():
        id_start, id_end, distance = row['id_start'], row['id_end'], row['distance']
        distance_matrix.at[id_start, id_end] = distance
        distance_matrix.at[id_end, id_start] = distance  # Symmetric

    # Cumulative distances along known routes
    for k in unique_ids:
        for i in unique_ids:
            for j in unique_ids:
                if distance_matrix.at[i, j] == 0 and i != j:
                    if distance_matrix.at[i, k] != 0 and distance_matrix.at[k, j] != 0:
                        distance_matrix.at[i, j] = distance_matrix.at[i, k] + distance_matrix.at[k, j]

    return distance_matrix

distance_matrix_df = calculate_distance_matrix(df3)
# print(distance_matrix.head())

'''task2. q2'''
## 2.

def unroll_distance_matrix(df)->pd.DataFrame():
    """
    Unroll a distance matrix to a DataFrame in the style of the initial dataset.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Unrolled DataFrame containing columns 'id_start', 'id_end', and 'distance'.
    """
    # Write your logic here

    unrolled_data = []
    distance_matrix = df

    for id_start in distance_matrix.index:
        for id_end in distance_matrix.columns:
            if id_start != id_end:
                distance = distance_matrix.at[id_start, id_end]
                unrolled_data.append({'id_start': id_start, 'id_end': id_end, 'distance': distance})

    return pd.DataFrame(unrolled_data)

unroll_distance_matrix_df = unroll_distance_matrix(distance_matrix_df)
# print(unroll_distance_matrix_df.head(8))

'''task2. q3'''

## 3.

def find_ids_within_ten_percentage_threshold(df, reference_id)->pd.DataFrame():
    """
    Find all IDs whose average distance lies within 10% of the average distance of the reference ID.

    Args:
        df (pandas.DataFrame)
        reference_id (int)

    Returns:
        pandas.DataFrame: DataFrame with IDs whose average distance is within the specified percentage threshold
                          of the reference ID's average distance.
    """
    # Write your logic here

    # Filter rows where id_start is the reference_id
    reference_rows = df[df['id_start'] == reference_id]

    # Calculate the average distance for the reference_id
    reference_avg_distance = reference_rows['distance'].mean()

    # Calculate the percentage threshold
    percentage_threshold = 0.1  # 10%

    # Filter rows where the average distance is within the percentage threshold
    result_df = df.groupby('id_start')['distance'].mean().reset_index()
    result_df = result_df[
        (result_df['distance'] >= (1 - percentage_threshold) * reference_avg_distance) &
        (result_df['distance'] <= (1 + percentage_threshold) * reference_avg_distance)
    ]

    return result_df

find_ids_within_ten_percentage_threshold_df = find_ids_within_ten_percentage_threshold(unroll_distance_matrix_df,1001408)
# print(find_ids_within_ten_percentage_threshold_df)

'''task2 q4.'''

## 4.

def calculate_toll_rate(df)->pd.DataFrame():
    """
    Calculate toll rates for each vehicle type based on the unrolled DataFrame.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Wrie your logic here
    
    # Copy the input DataFrame to avoid modifying the original
    toll_dataframe = df.copy()

    # Define rate coefficients for each vehicle type
    rate_coefficients = {'moto': 0.8,'car': 1.2,'rv': 1.5,'bus': 2.2,'truck': 3.6}

    # Add columns for each vehicle type with calculated toll rates
    for vehicle_type, rate_coefficient in rate_coefficients.items():
        toll_dataframe[vehicle_type] = toll_dataframe['distance'] * rate_coefficient

    return toll_dataframe

calculate_toll_rate_df = calculate_toll_rate(unroll_distance_matrix_df) 
# print(calculate_toll_rate_df)


'''task2. q5.'''

##5. 

def calculate_time_based_toll_rates(df)->pd.DataFrame():
    """
    Calculate time-based toll rates for different time intervals within a day.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Write your logic here

    return df
