import requests
import gtfs_realtime_pb2
from flask import current_app

url = 'https://gtfs.ztp.krakow.pl/VehiclePositions_A.pb'
local_filename = 'vehicle_positions.pb'
days_of_week = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

def download_gtfs_realtime_file():

    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    else:
        raise Exception(f"Nie udało się pobrać pliku. Status code: {response.status_code}")

def load_gtfs_data():
    download_gtfs_realtime_file()
    with open(local_filename, 'rb') as f:
        data = f.read()
    
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(data)
    
    vehicles = []
    for entity in feed.entity:
        if entity:
            vehicle = entity.vehicle
            vehicle_info = {field.name: getattr(vehicle, field.name) for field in vehicle.DESCRIPTOR.fields}
            vehicles.append(vehicle_info)
    
    return vehicles

def get_vehicle_with_route_name():
    download_gtfs_realtime_file()

    gtfs_realtime_vehicles = load_gtfs_data()
    gtfs_data = current_app.config['GTFS_DATA']

    if 'route_id' not in gtfs_data['routes'].index.names:
        gtfs_data['routes'].set_index('route_id', inplace=True)

    vehicle_list = []
    for cursor in gtfs_realtime_vehicles:
        trip_id = cursor['trip'].trip_id
        route_id = gtfs_data['trips'].loc[trip_id]['route_id']
        route_short_name = gtfs_data['routes'].loc[route_id]['route_short_name']
        trip_headsign = gtfs_data['trips'].loc[str(trip_id)]['trip_headsign']   
        shape_id = gtfs_data['trips'].loc[str(trip_id)]['shape_id']
        
        vehicle = {
            'vehicle_id' : cursor['vehicle'].license_plate,
            'route_short_name' : route_short_name,
            'latitude' : cursor['position'].latitude,
            'longitude' : cursor['position'].longitude,
            'timestamp' : cursor['timestamp'],
            'stop_id' : cursor['stop_id'],
            'trip_id' : trip_id,
            'route_id' : route_id,
            'trip_headsign' : trip_headsign,
            'shape_id' : shape_id,
            'bearing' : cursor['position'].bearing
        }
        vehicle_list.append(vehicle)
    return vehicle_list

def get_bus_schedule_data(route_id):
    gtfs_data = current_app.config['GTFS_DATA']
    filtered_data = gtfs_data['trips'][gtfs_data['trips']['route_id'] == route_id].copy()
    if 'trip_id' in filtered_data.index.names:
        filtered_data.reset_index(inplace=True)
    
    filtered_data['block_prefix'] = filtered_data['trip_id'].str.split('_').str[:2].str.join('_')
    unique_blocks = filtered_data[['block_prefix']].drop_duplicates().reset_index(drop=True)
        
    block_prefixes = unique_blocks['block_prefix']

    trips_data = gtfs_data['trips']
    stop_times_data = gtfs_data['stop_times']
    if 'trip_id' in stop_times_data.index.names:
        stop_times_data.reset_index(inplace=True)

    block_filtered_trips = trips_data[trips_data.index.str.startswith(block_prefixes.iloc[0] + '_t')]
    service_data = gtfs_data['calendar']
    service_id_2 = 'service_1'


    # days_with_service = service_day[days_of_week].loc[:, service_day[days_of_week].iloc[0] == 1].columns.tolist()


    route_bus_schedule_list = []

    for block_prefix in block_prefixes:
        
        block_filtered_trips = trips_data[trips_data.index.str.startswith(block_prefix + '_t')]

        first_trip_id = block_filtered_trips.iloc[0].name
        filtred_start_time_data = stop_times_data[gtfs_data['stop_times']['trip_id'] == first_trip_id]
        start_time = filtred_start_time_data.departure_time.values[0]
        last_trip_id = block_filtered_trips.iloc[-1].name
        filtred_end_time_data = stop_times_data[gtfs_data['stop_times']['trip_id'] == last_trip_id]
        end_time = filtred_end_time_data.departure_time.values[-1]

        service_id = block_filtered_trips["service_id"].values[0]

        service_day = gtfs_data['calendar'][gtfs_data['calendar']['service_id'] == service_id].copy()
        days_with_service = service_day[days_of_week].loc[:, service_day[days_of_week].iloc[0] == 1].columns.tolist()
        bus_schedule_dict = {
            'block_prefix' : block_prefix,
            'start_time' : start_time,
            'end_time' : end_time,
            'route_schedule' : block_filtered_trips,
            'service_days' : days_with_service
        }
        route_bus_schedule_list.append(bus_schedule_dict)

    return route_bus_schedule_list
