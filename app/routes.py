import requests
from flask import Blueprint, jsonify, request, current_app
from .gtfs_realtime_services import load_gtfs_data, get_vehicle_with_route_name
from .gtfs_processing import get_schedule_data, get_routes_list, get_stops_list, get_schedule_number_from_trip_id
from .mongo_connection import save_data_to_database, get_vehicle_history_data, get_route_history_data
import json
from flask import Response
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
    routes_list = get_routes_list()
    routes_dict = routes_list.to_dict(orient='records')
    formatted_json = json.dumps(routes_dict, indent=4)    

    return Response(formatted_json, mimetype='application/json')

@bp.route('/api/stops', methods=['GET'])
def get_stops_for_route():
    route_number = request.args.get('route_number')
    direction = request.args.get('direction')

    if route_number is None:
        return jsonify({"error": "route_number parameter is required"}), 400

    if direction not in ['0', '1']:
        return jsonify({"error": "direction parameter must be 0 or 1"}), 400
    
    stops_list = get_stops_list(route_number, direction)
    formatted_json = json.dumps(stops_list, indent=4)

    return Response(formatted_json, mimetype='application/json')


@bp.route('/api/realtime', methods=['GET'])
def get_realtime_data():    
    try:
        vehicle_positions = load_gtfs_data()
        json_serializable_data = convert_vehicle_positions_for_json(vehicle_positions)
        formatted_json = json.dumps(json_serializable_data, indent=4)

        return Response(formatted_json, mimetype='application/json')
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@bp.route('/api/realtime/vehicles', methods=['GET'])
def get_realtime_vehicles():   
    try:
        vehicle_list = get_vehicle_with_route_name()
        json_serializable_data = convert_vehicle_positions_for_json(vehicle_list)
        formatted_json = json.dumps(json_serializable_data, indent=4)

        return Response(formatted_json, mimetype='application/json')
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

@bp.route('/api/vehicles/schedule', methods=['GET'])
def get_schedule():
    route_id = request.args.get('route_id')
    vehicle_type = request.args.get('vehicle_type', 'bus')  

    if vehicle_type not in ['bus', 'tram']:
        return jsonify({'error': 'Invalid vehicle type. Must be "bus" or "tram".'}), 400

    schedule_data = get_schedule_data(route_id, vehicle_type)

    if isinstance(schedule_data, pd.DataFrame):
        json_serializable_data = convert_vehicle_positions_for_json(schedule_data)
    else:
        json_serializable_data = convert_vehicle_positions_for_json(schedule_data)
    
    formatted_json = json.dumps(json_serializable_data, indent=4)

    return Response(formatted_json, mimetype='application/json')

@bp.route('/save-data', methods=['GET'])
def save_data():
    try:
        save_data_to_database()
        response_data = {"message": "Saving data to database"}
        formatted_json = json.dumps(response_data, indent=4)
        return Response(formatted_json, mimetype='application/json')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@bp.route('/api/vehicles/history', methods=['GET'])
def get_vehicles_history_data():
    vehicle_id = request.args.get('vehicle_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    try:
        result = get_vehicle_history_data(vehicle_id, start_date=start_date, end_date=end_date)
        formatted_json = json.dumps(result, indent=4)
        return Response(formatted_json, mimetype='application/json')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@bp.route('/api/route/history', methods=['GET'])
def get_routes_history_data():
    route_name = request.args.get('route_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    try:
        result = get_route_history_data(route_name, start_date=start_date, end_date=end_date)
        formatted_json = json.dumps(result, indent=4)
        return Response(formatted_json, mimetype='application/json')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@bp.route('/api/vehicles/schedule/number', methods=['GET'])
def get_schedule_number():
    trip_id = request.args.get('trip_id')
    try:
        schedule_number = get_schedule_number_from_trip_id(trip_id)        
        formatted_json = json.dumps(schedule_number, indent=4)
        return Response(formatted_json, mimetype='application/json')
    except Exception as e:
        return jsonify({"error": str(e)}), 500
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
