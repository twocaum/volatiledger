# Volatiledger

Este projeto agora está estruturado de forma modular, separando responsabilidades em diferentes arquivos para melhorar a manutenção e a organização do código. Temos um script que coleta e salva dados de opções da Binance usando a API, armazenando os resultados em um banco de dados MongoDB, bem como uma interface de dashboard para visualizar esses dados.

## Requisitos

- Python 3.8 ou superior
- Docker (MongoDB)
- Conta na Binance e chave de API

## Instalação

1. Clone este repositório:
   ```bash
   git clone <url_do_repositorio>
   cd <nome_do_repositorio>
   ```

2. Crie e ative um ambiente virtual:
   ```bash
   python -m venv venv
   source venv/bin/activate # Para Linux/Mac
   .\venv\Scripts\activate # Para Windows
   ```

3. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```

4. Crie um arquivo `.env` na raiz do projeto e adicione suas variáveis de ambiente:
   ```env
   MONGO_URI=mongodb://root:example@localhost:27017/
   API_KEY=sua_chave_da_api
   ```

## Build e Execução

Este projeto possui scripts separados para construção e execução do ambiente Docker.

### Build do Ambiente

Para construir a imagem Docker do MongoDB personalizada, execute:
```bash
.\build.ps1
```

### Executar o Projeto

Para executar o container MongoDB e o projeto, utilize:
```bash
.\run.ps1
```

Ou, em sistemas Unix:
```bash
./build.sh && ./run.sh
```

## Uso

O projeto está dividido em diferentes componentes:

### API para Coleta e Processamento de Dados

- `api/api_client.py`: Coleta e salva os dados de opções e futuros da Binance no MongoDB.
- `api.py`: API Flask que fornece endpoints para acessar os dados armazenados, incluindo um endpoint para baixar o CSV completo.
- `utils.py`: Funções auxiliares para conectar ao MongoDB, processar dados e coletar informações da Binance.

Para executar a coleta de dados e a API:
```bash
python api/api_client.py
```

### Dashboard para Visualização dos Dados

- `dash_app.py`: Aplicação Dash para visualização dos dados de opções e futuros da Binance.
- `main.py`: Ponto de entrada para iniciar a aplicação Dash.

Para executar o dashboard:
```bash
python main.py
```

## Estrutura do Projeto

- `api/`: Diretório contendo os scripts da API e do cliente.
  - `api_client.py`: Script principal que coleta e salva os dados no MongoDB.
  - `api.py`: API Flask para servir os dados.
- `dash_app.py`: Aplicação Dash para visualização dos dados.
- `utils.py`: Funções utilitárias, como conexão com o MongoDB e processamento de dados.
- `requirements.txt`: Lista de dependências do projeto.
- `.env`: Arquivo contendo variáveis de ambiente (não incluído no repositório).
- `build.ps1` / `run.ps1`: Scripts PowerShell para construção e execução em sistemas Windows.
- `build.sh` / `run.sh`: Scripts Bash para construção e execução em sistemas Unix.

## Observações

- Certifique-se de configurar o `MONGO_URI` corretamente, especialmente se estiver usando credenciais personalizadas.
- A chave da API deve ser válida para acessar os dados da Binance.
- Respeite os limites de taxa da API da Binance, fazendo ajustes no tempo de espera (`time.sleep`) se necessário.
- Caso utilize o CSV, verifique se ele está no formato esperado para evitar erros na inserção dos dados.

## Licença

Este projeto está sob a licença MIT.

