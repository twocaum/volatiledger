#!/bin/bash

# Script Bash para construir a imagem Docker e preparar o contêiner MongoDB

# Definir variáveis
container_name="mongodb_container"
image_name="mongodb_custom"
mongo_port=27017

# Construir a imagem Docker a partir do Dockerfile
echo "Construindo a imagem Docker..."
docker build -t $image_name .

# Verificar se já existe um contêiner com o mesmo nome e removê-lo
if [ $(docker ps -a --format "{{.Names}}" | grep -w $container_name) ]; then
    echo "Removendo contêiner existente..."
    docker rm -f $container_name
fi

# Executar o contêiner Docker
echo "Executando o contêiner Docker..."
docker run --name $container_name -d -p $mongo_port:27017 $image_name

# Exibir status do contêiner
echo "Status do contêiner:"
docker ps | grep $container_name
