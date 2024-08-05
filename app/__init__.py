
import os
from flask import Flask
from .data_loader import load_gtfs_data

def create_app():
    app = Flask(__name__)

    extracted_path = os.path.join(os.path.dirname(__file__), '..', 'gtfs_data')

    app.config['GTFS_DATA'] = load_gtfs_data(extracted_path)

    from .routes import configure_routes
    configure_routes(app)

    return app
