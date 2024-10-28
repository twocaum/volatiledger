# build.ps1

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

# Construir a imagem Docker a partir do Dockerfile
Write-Output "Construindo a imagem Docker do MongoDB..."
docker build -t mongodb_custom .

# Verificar se já existe um contêiner MongoDB com o mesmo nome e removê-lo (caso exista)
if (docker ps -a --format "{{.Names}}" | Select-String -Pattern "^mongodb_container$") {
    Write-Output "Removendo contêiner MongoDB existente..."
    docker rm -f mongodb_container
}

# Verificar se já existe um contêiner Mongo-Express com o mesmo nome e removê-lo (caso exista)
if (docker ps -a --format "{{.Names}}" | Select-String -Pattern "^mongoexpress$") {
    Write-Output "Removendo contêiner Mongo-Express existente..."
    docker rm -f mongoexpress
}

# Instalar dependências do Python
if (Test-Path requirements.txt) {
    Write-Output "Instalando dependências do Python..."
    pip install -r requirements.txt
    pip install binance-futures-connector
} else {
    Write-Output "Arquivo requirements.txt não encontrado. Verifique o caminho e tente novamente."
}

Write-Output "Construção e configuração concluídas com sucesso."
