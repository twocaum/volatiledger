from flask import Flask, send_file, jsonify, request
import os
import logging
from utils import csv_file_path, fetch_data, collection_csv, collection_historical_exercise
import pandas as pd

# Initialize Flask app
app = Flask(__name__)

# Configure logging for better debugging
logging.basicConfig(level=logging.INFO)


@app.route('/api/csv_data', methods=['GET'])
def get_csv_data():
    """Endpoint to get data from the CSV collection."""
    df_csv = fetch_data(collection_csv)
    if not df_csv.empty:
        logging.info("CSV data found, sending JSON response.")
        data = df_csv.to_dict(orient='records')
        return jsonify(data)
    else:
        logging.warning("No data found in CSV collection.")
        return jsonify({"message": "Nenhum dado encontrado na coleção CSV."}), 404

@app.route('/api/historical_exercise_data', methods=['GET'])
def get_historical_exercise_data():
    """Endpoint to get data from the Historical Exercise Records collection."""
    df_historical_exercise = fetch_data(collection_historical_exercise)
    if not df_historical_exercise.empty:
        logging.info("Historical Exercise data found, sending JSON response.")
        data = df_historical_exercise.to_dict(orient='records')
        return jsonify(data)
    else:
        logging.warning("No data found in Historical Exercise Records collection.")
        return jsonify({"message": "Nenhum dado encontrado na coleção de Historical Exercise Records."}), 404

