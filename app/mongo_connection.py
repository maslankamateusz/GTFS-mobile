from pymongo import MongoClient
from datetime import datetime
from .gtfs_realtime_services import get_vehicle_with_route_name
from .gtfs_processing import get_route_name_from_trip_id, get_schedule_number_from_trip_id_arr

def get_mongo_client():
    connection_string = "mongodb+srv://mateuszmaslanka06:i2w30W6qmPOrY3z8@transport-gtfs.3tvjz.mongodb.net/?retryWrites=true&w=majority&appName=transport-gtfs"
    client = MongoClient(connection_string)
    return client

def get_database(client, db_name):
    return client[db_name]

def create_collection(db, collection_name):
    collection = db[collection_name]
    return collection

def connect_to_database():
    client = get_mongo_client()
    db = get_database(client, "mpk-gtfs")

    collection_name = "vehicles-history"
    collection = create_collection(db, collection_name)
    return collection

def get_current_vehicle_list():
    vehicles_data = get_vehicle_with_route_name()
    vehicles_list = []
    for vehicle in vehicles_data:
        full_trip_id = vehicle['trip_id']

        vehicle_dict = {
            'vehicle_id' : vehicle['vehicle_id'],
            'route_short_name' : get_route_name_from_trip_id(vehicle['trip_id'], vehicle['vehicle_id']),
            'trip_id' : full_trip_id,
        }
        vehicles_list.append(vehicle_dict)

    filled_vehicle_list = get_schedule_number_from_trip_id_arr(vehicles_list)

    return filled_vehicle_list


def save_data_to_database():

    client = get_mongo_client()
    db = get_database(client, "mpk-gtfs")
    check_data_from_database(db)
    client.close()
    return db

def check_data_from_database(db):
    current_datetime = datetime.now()
    current_date = current_datetime.strftime('%Y-%m-%d')
    current_timestamp = current_datetime.timestamp()
    collection_name = "vehicles-history"
    collection = db[collection_name]

    result = collection.find_one({'date': current_date})

    if result:
        document_timestamp = result.get('timestamp', None)  
        if document_timestamp is not None:
            if current_timestamp - document_timestamp > 10 * 60:
                vehicle_list = result['vehicle_list'] 
                update_data(vehicle_list)
            else:
                print("Timestamp jest młodszy niż 10 minut")
        else:
            print("Brak znacznika czasu w dokumentu")
    else:
        add_data_to_database()
        print("Brak dokumentu z datą:", current_date)

def update_data(vehicle_list):

    current_vehicle_list = get_current_vehicle_list()

    current_vehicle_list_sorted = sorted(current_vehicle_list, key=lambda x: x['vehicle_id'])
    vehicle_list_sorted = sorted(vehicle_list, key=lambda x: x['vehicle_id'])

    if current_vehicle_list_sorted != vehicle_list_sorted:
        for current_vehicle in current_vehicle_list:
            vehicle_id = current_vehicle['vehicle_id']
            
            matching_vehicle = next((v for v in vehicle_list if v['vehicle_id'] == vehicle_id), None)
            
            if matching_vehicle:
                if current_vehicle['route_short_name'][0] not in matching_vehicle['route_short_name']:
                    matching_vehicle['route_short_name'].extend(current_vehicle['route_short_name'])

            else:
                vehicle_list.append(current_vehicle)
       
        add_update_data_to_database(vehicle_list)



def add_update_data_to_database(vehicle_list):
    current_datetime = datetime.now()

    data = {
        'date': current_datetime.strftime('%Y-%m-%d'),
        'time': current_datetime.strftime('%H:%M:%S'),
        'timestamp': current_datetime.timestamp(),
        'vehicle_list': vehicle_list
    }

    collection = connect_to_database()

    existing_doc = collection.find_one({'date': data['date']})

    if existing_doc:
        collection.update_one(
            {'date': data['date']},
            {'$set': {
                'time': data['time'],
                'timestamp': data['timestamp'],
                'vehicle_list': vehicle_list
            }}
        )
    else:
       add_data_to_database()


def add_data_to_database():
    current_datetime = datetime.now()

    vehicles_list = get_current_vehicle_list()
    data = {
        'date': current_datetime.strftime('%Y-%m-%d'),
        'time': current_datetime.strftime('%H:%M:%S'),
        'timestamp': current_datetime.timestamp(),
        'vehicle_list' : vehicles_list
    }

    collection = connect_to_database()
    
    result = collection.insert_one(data)

    return f"Document inserted with ID: {result.inserted_id}"

def get_vehicle_history_data(vehicle_id, start_date=None, end_date=None):
    collection = connect_to_database()
    query = {}
    
    if start_date and end_date:
        query["date"] = {"$gte": start_date, "$lte": end_date}
    elif start_date:
        query["date"] = {"$gte": start_date}
    elif end_date:
        query["date"] = {"$lte": end_date}

    projection = {
        "_id": 0, 
        "date": 1, 
        "vehicle_list": 1  
    }
    
    vehicle_history = collection.find(query, projection)
    vehicle_history_list = list(vehicle_history)

    filtered_history = []
    for entry in vehicle_history_list:
        filtered_vehicles = [
            vehicle for vehicle in entry['vehicle_list'] 
            if vehicle['vehicle_id'] == vehicle_id
        ]
        if filtered_vehicles:
            filtered_history.append({
                "date": entry["date"],
                "vehicle_list": filtered_vehicles
            })

    return filtered_history


def get_route_history_data(route_name, start_date=None, end_date=None):
    collection = connect_to_database()

    query = {}
    
    if start_date and end_date:
        query["date"] = {"$gte": start_date, "$lte": end_date}
    elif start_date:
        query["date"] = {"$gte": start_date}
    elif end_date:
        query["date"] = {"$lte": end_date}

    projection = {
        "_id": 0, 
        "date": 1, 
        "vehicle_list": 1  
    }

    vehicle_history = collection.find(query, projection)
    vehicle_history_list = list(vehicle_history)

    filtered_history = []
    for entry in vehicle_history_list:
        filtered_vehicles = [vehicle for vehicle in entry['vehicle_list'] if route_name in vehicle['route_short_name']]
        if filtered_vehicles:
            filtered_history.append({
                "date": entry["date"],
                "vehicle_list": filtered_vehicles
            })

    return filtered_history
