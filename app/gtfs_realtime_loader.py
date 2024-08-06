import requests
import gtfs_realtime_pb2

def download_file(url, local_filename):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    else:
        raise Exception(f"Nie udało się pobrać pliku. Status code: {response.status_code}")

def load_vehicle_positions(file_path):
    with open(file_path, 'rb') as f:
        data = f.read()
    
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(data)
    
    vehicles = []
    for entity in feed.entity:
        if entity.HasField('vehicle'):
            vehicle = entity.vehicle
            vehicle_info = {field.name: getattr(vehicle, field.name) for field in vehicle.DESCRIPTOR.fields}
            vehicles.append(vehicle_info)
    
    return vehicles

def get_vehicle_positions(url):
    local_filename = 'vehicle_positions.pb'
    
    download_file(url, local_filename)
    
    return load_vehicle_positions(local_filename)
