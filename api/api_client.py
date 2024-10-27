# api.py

from flask import Flask, send_file, jsonify, request
import os
from utils import csv_file_path
from utils import (
    fetch_data,
    collection_csv,
    collection_options,
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

@app.route('/api/options_data', methods=['GET'])
def get_options_data():
    df_options = fetch_data(collection_options)
    if not df_options.empty:
        data = df_options.to_dict(orient='records')
        return jsonify(data)
    else:
        return jsonify({"message": "Nenhum dado encontrado na coleção de opções."}), 404

@app.route('/api/futures_data', methods=['GET'])
def get_futures_data():
    df_futures = fetch_data(collection_futures)
    if not df_futures.empty:
        data = df_futures.to_dict(orient='records')
        return jsonify(data)
    else:
        return jsonify({"message": "Nenhum dado encontrado na coleção de futuros."}), 404

# Rota para buscar dados filtrados por parâmetros (exemplo)
@app.route('/api/options_data/filter', methods=['GET'])
def filter_options_data():
    symbol = request.args.get('symbol')
    start_time = request.args.get('start_time')  # Espera timestamp em milissegundos
    end_time = request.args.get('end_time')      # Espera timestamp em milissegundos

    df_options = fetch_data(collection_options)

    if df_options.empty:
        return jsonify({"message": "Nenhum dado encontrado na coleção de opções."}), 404

    if symbol:
        df_options = df_options[df_options['symbol'] == symbol]

    if start_time:
        df_options = df_options[df_options['time'] >= pd.to_datetime(int(start_time), unit='ms')]

    if end_time:
        df_options = df_options[df_options['time'] <= pd.to_datetime(int(end_time), unit='ms')]

    if df_options.empty:
        return jsonify({"message": "Nenhum dado encontrado com os critérios fornecidos."}), 404

    data = df_options.to_dict(orient='records')
    return jsonify(data)
