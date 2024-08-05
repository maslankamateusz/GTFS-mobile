# app/data_loader.py

import pandas as pd
import os

def load_gtfs_data(extracted_path):
    stops_df = pd.read_csv(os.path.join(extracted_path, 'stops.txt'))
    routes_df = pd.read_csv(os.path.join(extracted_path, 'routes.txt'))
    trips_df = pd.read_csv(os.path.join(extracted_path, 'trips.txt'))
    stop_times_df = pd.read_csv(os.path.join(extracted_path, 'stop_times.txt'))
    
    routes_df['route_short_name'] = routes_df['route_short_name'].astype(str)
    trips_df.set_index('trip_id', inplace=True)
    stop_times_df.set_index('trip_id', inplace=True)
    
    return {
        'stops': stops_df,
        'routes': routes_df,
        'trips': trips_df,
        'stop_times': stop_times_df
    }
