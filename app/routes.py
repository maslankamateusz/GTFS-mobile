import requests
from flask import Blueprint, jsonify, request, current_app
from .gtfs_realtime_loader import load_gtfs_data, get_vehicle_with_route_name, get_bus_schedule_data
from .mongo_connection import save_data_to_database, get_vehicle_history_data
import pandas as pd


bp = Blueprint('main', __name__)

def download_file(url, local_filename):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    else:
        raise Exception(f"Nie udało się pobrać pliku. Status code: {response.status_code}")

@bp.route('/api/routes')
def get_routes():
    gtfs_data = current_app.config['GTFS_DATA']
    routes = gtfs_data['routes'][['route_id', 'route_short_name']]
    routes_dict = routes.to_dict(orient='records')
    return jsonify(routes_dict)

@bp.route('/api/stops', methods=['GET'])
def get_stops_for_route():
    route_number = request.args.get('route_number')
    direction = request.args.get('direction')

    if route_number is None:
        return jsonify({"error": "route_number parameter is required"}), 400

    if direction not in ['0', '1']:
        return jsonify({"error": "direction parameter must be 0 or 1"}), 400

    gtfs_data = current_app.config['GTFS_DATA']

    route = gtfs_data['routes'][gtfs_data['routes']['route_short_name'] == str(route_number)]
    if route.empty:
        return jsonify({"error": "No route found for the given route_number"}), 404

    route_id = route.iloc[0]['route_id']

    trips_for_route = gtfs_data['trips'][gtfs_data['trips']['route_id'] == route_id]
    if trips_for_route.empty:
        return jsonify({"error": "No trips found for the given route_id"}), 404

    filtered_trips = trips_for_route[trips_for_route['direction_id'] == int(direction)]
    trip_ids = filtered_trips.index.unique()

    stops_for_all_trips = gtfs_data['stop_times'].loc[trip_ids]
    stops_for_all_trips = stops_for_all_trips.reset_index().merge(gtfs_data['stops'], on='stop_id')
    stops = stops_for_all_trips[['stop_id', 'stop_name']].drop_duplicates().to_dict(orient='records')

    return jsonify(stops)

@bp.route('/api/realtime', methods=['GET'])
def get_realtime_data():
    url = 'https://gtfs.ztp.krakow.pl/VehiclePositions_A.pb'
    
    try:
        vehicle_positions = load_gtfs_data()
        json_serializable_data = convert_vehicle_positions_for_json(vehicle_positions)

        return jsonify(json_serializable_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@bp.route('/api/realtime/vehicles', methods=['GET'])
def get_realtime_vehicles():   
    try:
        vehicle_list =  get_vehicle_with_route_name()
        json_serializable_data = convert_vehicle_positions_for_json(vehicle_list)

        return jsonify(json_serializable_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@bp.route('/api/vehicles/schedule', methods=['GET'])
def get_bus_schedule():
    route_id = request.args.get('route_id')
    bus_schedule_data = get_bus_schedule_data(route_id)
    if isinstance(bus_schedule_data, pd.DataFrame):
        json_serializable_data = convert_vehicle_positions_for_json(bus_schedule_data)
    else:
        json_serializable_data = convert_vehicle_positions_for_json(bus_schedule_data)
    return jsonify(json_serializable_data)
@bp.route('/save-data', methods=['GET'])
def save_data():
    save_data_to_database()
    
    return jsonify('Saving data to database')

@bp.route('/api/vehicles/history', methods=['GET'])
def get_vehicles_history_data():
    vehicle_id = request.args.get('vehicle_id')
    print(vehicle_id)
    result = get_vehicle_history_data(vehicle_id)
    return result


def convert_value(value):
    if isinstance(value, (bytes, bytearray)):
        return value.decode('utf-8')
    elif isinstance(value, (list, tuple)):
        return [convert_value(v) for v in value]
    elif isinstance(value, dict):
        return {k: convert_value(v) for k, v in value.items()}
    elif hasattr(value, 'ListFields'):
        return {field.name: convert_value(getattr(value, field.name)) for field in value.DESCRIPTOR.fields}
    elif hasattr(value, 'extend'):
        return [convert_value(v) for v in value]
    elif isinstance(value, pd.DataFrame):
        return value.to_dict(orient='records')  
    return value

def convert_vehicle_positions_for_json(vehicle_positions):
    if isinstance(vehicle_positions, pd.DataFrame):
        return vehicle_positions.to_dict(orient='records')  
    return [convert_value(vehicle) for vehicle in vehicle_positions]


def configure_routes(app):
    app.register_blueprint(bp)
