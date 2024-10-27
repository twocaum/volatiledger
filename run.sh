#!/bin/bash

# Script Bash para instalar dependências e executar o API client

# Instalar dependências do Python
echo "Instalando dependências do Python..."
pip install -r requirements.txt

# Executar o script Python do API client
echo "Executando o API client..."
python3 api/api_client.py
