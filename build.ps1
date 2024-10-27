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

# Construir a imagem Docker a partir do Dockerfile
Write-Output "Construindo a imagem Docker..."
docker build -t mongodb_custom .

# Verificar se já existe um contêiner com o mesmo nome e removê-lo (caso exista)
if (docker ps -a --format "{{.Names}}" | Select-String -Pattern mongodb_container) {
    Write-Output "Removendo contêiner existente..."
    docker rm -f mongodb_container
}

# Instalar dependências do Python
Write-Output "Instalando dependências do Python..."
pip install -r requirements.txt

Write-Output "Construção e configuração concluídas com sucesso."
