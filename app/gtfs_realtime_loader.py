import requests
import gtfs_realtime_pb2

def download_file(url, local_filename):
    """Pobiera plik z internetu i zapisuje go lokalnie."""
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    else:
        raise Exception(f"Nie udało się pobrać pliku. Status code: {response.status_code}")

def load_vehicle_positions(file_path):
    """Ładowanie danych o położeniu pojazdów z pliku .pb i zwracanie listy słowników."""
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
    """Pobiera plik z URL i zwraca dane o położeniu pojazdów jako listę słowników."""
    local_filename = 'vehicle_positions.pb'
    
    # Pobierz plik z internetu
    download_file(url, local_filename)
    
    # Załaduj i zwróć dane o położeniu pojazdów
    return load_vehicle_positions(local_filename)
