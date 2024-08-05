from flask import Blueprint, jsonify, request, current_app

bp = Blueprint('main', __name__)

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

    print(f"Route number: {route_number}, Direction: {direction}")

    return jsonify(stops)

def configure_routes(app):
    app.register_blueprint(bp)
