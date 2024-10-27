# Usar a imagem oficial do MongoDB como base
FROM mongo:latest

# Definir o diretório de trabalho
WORKDIR /data

# Expor a porta padrão do MongoDB
EXPOSE 27017

# Comando para iniciar o MongoDB
CMD ["mongod"]
