# run.ps1

# Carregar variáveis do arquivo .env
if (Test-Path .env) {
    Write-Output "Carregando variáveis do arquivo .env..."
    Get-Content .env | ForEach-Object {
        if ($_ -match "(.+?)=(.+)") {
            $name = $matches[1]
            $value = $matches[2]
            Write-Output "Definindo variável de ambiente: $name"
            Set-Item -Path "Env:\$name" -Value "$value"
        }
    }
} else {
    Write-Output "Arquivo .env não encontrado. Verifique o caminho e tente novamente."
    exit
}

# Verificar se a rede Docker existe, senão criar
if (-not (docker network ls --format "{{.Name}}" | Select-String -Pattern "^mongo-network$")) {
    Write-Output "Criando rede mongo-network..."
    docker network create mongo-network
} else {
    Write-Output "Rede mongo-network já existe."
}

# Verificar se o contêiner MongoDB já existe, senão criar
if (docker ps -a --format "{{.Names}}" | Select-String -Pattern "^mongodb_container$") {
    Write-Output "Removendo contêiner MongoDB existente..."
    docker rm -f mongodb_container
}

Write-Output "Executando o contêiner MongoDB..."
docker run --name mongodb_container --network mongo-network -d -p 27017:27017 `
    -e MONGO_INITDB_ROOT_USERNAME=$Env:MONGO_INITDB_ROOT_USERNAME `
    -e MONGO_INITDB_ROOT_PASSWORD=$Env:MONGO_INITDB_ROOT_PASSWORD `
    mongo:latest

# Verificar se o contêiner Mongo-Express já existe, senão criar
if (docker ps -a --format "{{.Names}}" | Select-String -Pattern "^mongoexpress$") {
    Write-Output "Removendo contêiner Mongo-Express existente..."
    docker rm -f mongoexpress
}

Write-Output "Executando o contêiner Mongo-Express..."
docker run --name mongoexpress --network mongo-network -d -p 8081:8081 `
    -e ME_CONFIG_MONGODB_ADMINUSERNAME=$Env:MONGO_INITDB_ROOT_USERNAME `
    -e ME_CONFIG_MONGODB_ADMINPASSWORD=$Env:MONGO_INITDB_ROOT_PASSWORD `
    -e ME_CONFIG_MONGODB_SERVER=mongodb_container `
    mongo-express:latest

# Verificar se os contêineres estão em execução
Start-Sleep -Seconds 5
Write-Output "Status dos contêineres:"
docker ps | Select-String -Pattern "mongodb_container|mongoexpress"

# Instalar dependências do Python (apenas se o ambiente estiver configurado)
if (Test-Path requirements.txt) {
    Write-Output "Instalando dependências do Python..."
    pip install -r requirements.txt
    pip install binance-futures-connector
} else {
    Write-Output "Arquivo requirements.txt não encontrado. Verifique o caminho e tente novamente."
}

# Executar o script Python do API client
if (Test-Path "api/main.py") {
    Write-Output "Executando o API client..."
    python api/main.py
} else {
    Write-Output "Arquivo 'api/main.py' não encontrado. Verifique o caminho e tente novamente."
}
