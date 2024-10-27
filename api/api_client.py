# api.py

from flask import Flask, send_file, jsonify, request
import os
from utils import csv_file_path
from utils import (
    fetch_data,
    collection_csv,
    collection_open_interest,
    collection_futures,
)
import pandas as pd

app = Flask(__name__)

@app.route('/api/dados_completos', methods=['GET'])
def download_completo():
    if os.path.exists(csv_file_path):
        return send_file(csv_file_path, as_attachment=True)
    else:
        return "Arquivo CSV completo não encontrado.", 404

@app.route('/api/csv_data', methods=['GET'])
def get_csv_data():
    df_csv = fetch_data(collection_csv)
    if not df_csv.empty:
        data = df_csv.to_dict(orient='records')
        return jsonify(data)
    else:
        return jsonify({"message": "Nenhum dado encontrado na coleção CSV."}), 404


@app.route('/api/futures_data', methods=['GET'])
def get_futures_data():
    df_futures = fetch_data(collection_futures)
    if not df_futures.empty:
        data = df_futures.to_dict(orient='records')
        return jsonify(data)
    else:
        return jsonify({"message": "Nenhum dado encontrado na coleção de futuros."}), 404


