import requests
import gtfs_realtime_pb2
from flask import current_app

url_a = 'https://gtfs.ztp.krakow.pl/VehiclePositions_A.pb'
local_filename_a = 'vehicle_positions_a.pb'
url_t = 'https://gtfs.ztp.krakow.pl/VehiclePositions_T.pb'
local_filename_t = 'vehicle_positions_t.pb'
days_of_week = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

def download_gtfs_realtime_file():
    response_a = requests.get(url_a, stream=True)
    if response_a.status_code == 200:
        with open(local_filename_a, 'wb') as f:
            for chunk in response_a.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    else:
        raise Exception(f"Nie udało się pobrać pliku. Status code: {response_a.status_code}")
    
    response_t = requests.get(url_t, stream=True)
    if response_t.status_code == 200:
        with open(local_filename_t, 'wb') as f:
            for chunk in response_t.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    else:
        raise Exception(f"Nie udało się pobrać pliku. Status code: {response_t.status_code}")

def load_gtfs_data():
    download_gtfs_realtime_file()
    
    vehicles_a = []
    with open(local_filename_a, 'rb') as f:
        data_a = f.read()
    
    feed_a = gtfs_realtime_pb2.FeedMessage()
    feed_a.ParseFromString(data_a)
    
    for entity_a in feed_a.entity:
        if entity_a:
            vehicle_a = entity_a.vehicle
            vehicle_info_a = {field.name: getattr(vehicle_a, field.name) for field in vehicle_a.DESCRIPTOR.fields}
            vehicles_a.append(vehicle_info_a)
    
    vehicles_t = []
    with open(local_filename_t, 'rb') as f:
        data_t = f.read()
    
    feed_t = gtfs_realtime_pb2.FeedMessage()
    feed_t.ParseFromString(data_t)
    
    for entity_t in feed_t.entity:
        if entity_t.HasField('vehicle'):
            vehicle_t = entity_t.vehicle
            vehicle_info_t = {field.name: getattr(vehicle_t, field.name) for field in vehicle_t.DESCRIPTOR.fields}
            vehicle_info_t['type'] = 'tram'
            vehicles_t.append(vehicle_info_t)

    return vehicles_a, vehicles_t

def get_vehicle_with_route_name():
    download_gtfs_realtime_file()

    vehicles_a, vehicles_t = load_gtfs_data()
    gtfs_data = current_app.config['GTFS_DATA']

    if 'route_id' not in gtfs_data['routes_a'].index.names:
        gtfs_data['routes_a'].set_index('route_id', inplace=True)

    if 'route_id' not in gtfs_data['routes_t'].index.names:
        gtfs_data['routes_t'].set_index('route_id', inplace=True)

    vehicle_list = []

    for cursor_a in vehicles_a:
        trip_id_a = cursor_a['trip'].trip_id
        if trip_id_a in gtfs_data['trips_a'].index:
            route_id_a = gtfs_data['trips_a'].loc[trip_id_a]['route_id']
            route_short_name_a = gtfs_data['routes_a'].loc[route_id_a]['route_short_name']
            trip_headsign_a = gtfs_data['trips_a'].loc[str(trip_id_a)]['trip_headsign']
            shape_id_a = gtfs_data['trips_a'].loc[str(trip_id_a)]['shape_id']

            vehicle_a = {
                'vehicle_id': cursor_a['vehicle'].license_plate,
                'route_short_name': route_short_name_a,
                'latitude': cursor_a['position'].latitude,
                'longitude': cursor_a['position'].longitude,
                'timestamp': cursor_a['timestamp'],
                'stop_id': cursor_a['stop_id'],
                'trip_id': trip_id_a,
                'route_id': route_id_a,
                'trip_headsign': trip_headsign_a,
                'shape_id': shape_id_a,
                'bearing': cursor_a['position'].bearing
            }
            vehicle_list.append(vehicle_a)
    
    for cursor_t in vehicles_t:
        trip_id_t = cursor_t['trip'].trip_id
        if trip_id_t in gtfs_data['trips_t'].index:
            route_id_t = gtfs_data['trips_t'].loc[trip_id_t]['route_id']
            route_short_name_t = gtfs_data['routes_t'].loc[route_id_t]['route_short_name']
            trip_headsign_t = gtfs_data['trips_t'].loc[str(trip_id_t)]['trip_headsign']
            shape_id_t = gtfs_data['trips_t'].loc[str(trip_id_t)]['shape_id']

            vehicle_t = {
                'vehicle_id': cursor_t['vehicle'].license_plate,
                'route_short_name': route_short_name_t,
                'latitude': cursor_t['position'].latitude,
                'longitude': cursor_t['position'].longitude,
                'timestamp': cursor_t['timestamp'],
                'stop_id': cursor_t['stop_id'],
                'trip_id': trip_id_t,
                'route_id': route_id_t,
                'trip_headsign': trip_headsign_t,
                'shape_id': shape_id_t,
                'bearing': cursor_t['position'].bearing,
                'type': 'tram'
            }
            vehicle_list.append(vehicle_t)

    return vehicle_list

def get_bus_schedule_data(route_id):
    gtfs_data = current_app.config['GTFS_DATA']
    
    filtered_data_a = gtfs_data['trips_a'][gtfs_data['trips_a']['route_id'] == route_id].copy()
    if 'trip_id' in filtered_data_a.index.names:
        filtered_data_a.reset_index(inplace=True)
    
    filtered_data_a['block_prefix'] = filtered_data_a['trip_id'].str.split('_').str[:2].str.join('_')
    unique_blocks_a = filtered_data_a[['block_prefix']].drop_duplicates().reset_index(drop=True)
        
    block_prefixes_a = unique_blocks_a['block_prefix']

    trips_data_a = gtfs_data['trips_a']
    stop_times_data_a = gtfs_data['stop_times_a']
    if 'trip_id' in stop_times_data_a.index.names:
        stop_times_data_a.reset_index(inplace=True)

    route_bus_schedule_list = []

    for block_prefix_a in block_prefixes_a:
        block_filtered_trips_a = trips_data_a[trips_data_a.index.str.startswith(block_prefix_a + '_t')]

        first_trip_id_a = block_filtered_trips_a.iloc[0].name
        filtred_start_time_data_a = stop_times_data_a[gtfs_data['stop_times_a']['trip_id'] == first_trip_id_a]
        start_time_a = filtred_start_time_data_a.departure_time.values[0]
        last_trip_id_a = block_filtered_trips_a.iloc[-1].name
        filtred_end_time_data_a = stop_times_data_a[gtfs_data['stop_times_a']['trip_id'] == last_trip_id_a]
        end_time_a = filtred_end_time_data_a.departure_time.values[-1]

        service_id_a = block_filtered_trips_a["service_id"].values[0]

        service_day_a = gtfs_data['calendar_a'][gtfs_data['calendar_a']['service_id'] == service_id_a].copy()
        days_with_service_a = service_day_a[days_of_week].loc[:, service_day_a[days_of_week].iloc[0] == 1].columns.tolist()

        bus_schedule_dict_a = {
            'block_prefix': block_prefix_a,
            'start_time': start_time_a,
            'end_time': end_time_a,
            'route_schedule': block_filtered_trips_a,
            'service_days': days_with_service_a
        }
        route_bus_schedule_list.append(bus_schedule_dict_a)

    return route_bus_schedule_list

def get_route_name_from_trip_id(trip_id):
    gtfs_data = current_app.config['GTFS_DATA']

    trips_data_a = gtfs_data['trips_a']
    routes_data_a = gtfs_data['routes_a']
    trips_data_t = gtfs_data['trips_t']
    routes_data_t = gtfs_data['routes_t']

    trip_id_prefix_a = "_".join(trip_id.split("_")[:3])   
    filtered_trips_a = trips_data_a[trips_data_a.index.str.startswith(trip_id_prefix_a)]
    unique_filtered_route_id_a = set(filtered_trips_a['route_id'].values)

    if 'route_id' in routes_data_a.index.names:
        routes_data_a.reset_index(inplace=True)

    route_name_list_a = []
    for route_id_a in unique_filtered_route_id_a:
        route_name_a = routes_data_a[routes_data_a['route_id'] == route_id_a]['route_short_name'].values.tolist()
        route_name_list_a.extend(route_name_a)

    trip_id_prefix_t = "_".join(trip_id.split("_")[:3])   
    filtered_trips_t = trips_data_t[trips_data_t.index.str.startswith(trip_id_prefix_t)]
    unique_filtered_route_id_t = set(filtered_trips_t['route_id'].values)

    if 'route_id' in routes_data_t.index.names:
        routes_data_t.reset_index(inplace=True)

    route_name_list_t = []
    for route_id_t in unique_filtered_route_id_t:
        route_name_t = routes_data_t[routes_data_t['route_id'] == route_id_t]['route_short_name'].values.tolist()
        route_name_list_t.extend(route_name_t)
    
    return route_name_list_a + route_name_list_t
