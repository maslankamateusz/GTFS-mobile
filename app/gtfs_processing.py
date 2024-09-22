from flask import current_app
import pandas as pd
days_of_week = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

def get_routes_list():
    gtfs_data = current_app.config['GTFS_DATA']
    routes_a = gtfs_data['routes_a'][['route_id', 'route_short_name']]
    routes_t = gtfs_data['routes_t'][['route_id', 'route_short_name']]
    routes_list = pd.concat([routes_a, routes_t], ignore_index=True)

    return routes_list

def get_stops_list(route_number, direction):
    gtfs_data = current_app.config['GTFS_DATA']

    routes_list = get_routes_list()
    route_id = routes_list['route_id'][routes_list['route_short_name'] == str(route_number)].values[0]

    if len(route_number) == 3:
        trips = gtfs_data['trips_a']
        stop_times = gtfs_data['stop_times_a']
        stops = gtfs_data['stops_a']
    else:
        trips = gtfs_data['trips_t']
        stop_times = gtfs_data['stop_times_t']
        stops = gtfs_data['stops_t']

    trips_for_route = trips[trips['route_id'] == route_id]
    if len(route_number) < 3:
        trips_for_route = trips_for_route.groupby('block_id').apply(lambda x: x.iloc[1:-1])
    
    filtered_trips = trips_for_route[trips_for_route['direction_id'] == int(direction)]
    
    if len(route_number) == 3:
        trip_ids = filtered_trips.index.unique()
    else:
        trip_ids = filtered_trips.index.get_level_values(1).unique()

    stops_for_all_trips = stop_times.loc[trip_ids].reset_index().merge(stops, on='stop_id')
    stops = stops_for_all_trips[['stop_id', 'stop_name']].drop_duplicates().to_dict(orient='records')

    return stops

    

def get_schedule_data(route_id, vehicle_type='bus'):
    gtfs_data = current_app.config['GTFS_DATA']

    if vehicle_type == 'bus':
        trips_data = gtfs_data['trips_a']
        stop_times_data = gtfs_data['stop_times_a']
        calendar_data = gtfs_data['calendar_a']
    elif vehicle_type == 'tram':
        trips_data = gtfs_data['trips_t']
        stop_times_data = gtfs_data['stop_times_t']
        calendar_data = gtfs_data['calendar_t']
    else:
        raise ValueError("Invalid vehicle type. Must be 'bus' or 'tram'.")

    filtered_data = trips_data[trips_data['route_id'] == route_id].copy()
    if 'trip_id' in filtered_data.index.names:
        filtered_data.reset_index(inplace=True)
    
    filtered_data['block_prefix'] = filtered_data['trip_id'].str.split('_').str[:2].str.join('_')
    unique_blocks = filtered_data[['block_prefix']].drop_duplicates().reset_index(drop=True)
        
    block_prefixes = unique_blocks['block_prefix']

    if 'trip_id' in stop_times_data.index.names:
        stop_times_data.reset_index(inplace=True)

    route_schedule_list = []

    for block_prefix in block_prefixes:
        block_filtered_trips = trips_data[trips_data.index.str.startswith(block_prefix + '_t')]

        first_trip_id = block_filtered_trips.iloc[0].name
        filtred_start_time_data = stop_times_data[stop_times_data['trip_id'] == first_trip_id]
        start_time = filtred_start_time_data.departure_time.values[0]
        last_trip_id = block_filtered_trips.iloc[-1].name
        filtred_end_time_data = stop_times_data[stop_times_data['trip_id'] == last_trip_id]
        end_time = filtred_end_time_data.departure_time.values[-1]

        service_id = block_filtered_trips["service_id"].values[0]

        service_day = calendar_data[calendar_data['service_id'] == service_id].copy()
        days_with_service = service_day[days_of_week].loc[:, service_day[days_of_week].iloc[0] == 1].columns.tolist()

        schedule_dict = {
            'block_prefix': block_prefix,
            'start_time': start_time,
            'end_time': end_time,
            'route_schedule': block_filtered_trips,
            'service_days': days_with_service
        }
        route_schedule_list.append(schedule_dict)

    return route_schedule_list


def get_route_name_from_trip_id(trip_id, vehicle_id):
    gtfs_data = current_app.config['GTFS_DATA']
    if vehicle_id[0] in ['H', 'R']:
        trips_data = gtfs_data['trips_t']
        routes_data = gtfs_data['routes_t']
    else:
        trips_data = gtfs_data['trips_a']
        routes_data = gtfs_data['routes_a']
    
    trip_id_prefix = "_".join(trip_id.split("_")[:3])   
    filtered_trips = trips_data[trips_data.index.str.startswith(trip_id_prefix)]
    unique_filtered_route_id = set(filtered_trips['route_id'].values)

    if 'route_id' in routes_data.index.names:
        routes_data.reset_index(inplace=True)

    route_name_list = []
    for route_id in unique_filtered_route_id:
        route_name = routes_data[routes_data['route_id'] == route_id]['route_short_name'].values.tolist()
        route_name_list.extend(route_name)

   
    return route_name_list