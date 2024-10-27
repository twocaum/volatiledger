
# Executar o contêiner Docker
Write-Output "Executando o contêiner Docker..."
docker run --name mongodb_container -d -p 27017:27017 -e MONGO_INITDB_ROOT_USERNAME=$Env:MONGO_INITDB_ROOT_USERNAME -e MONGO_INITDB_ROOT_PASSWORD=$Env:MONGO_INITDB_ROOT_PASSWORD mongodb_custom

# Exibir status do contêiner
Write-Output "Status do contêiner:"
docker ps | Select-String -Pattern mongodb_container

# Instalar dependências do Python
Write-Output "Instalando dependências do Python..."
pip install -r requirements.txt

# Executar o script Python do API client
Write-Output "Executando o API client..."
python api/main.py