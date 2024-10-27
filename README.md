# Volatiledger

Este projeto contém um script Python que coleta e salva dados de opções da Binance usando a API e armazena os resultados em um banco de dados MongoDB. O script utiliza variáveis de ambiente para configurar credenciais de acesso e a chave da API.

## Requisitos

- Python 3.8 ou superior
- MongoDB
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

Para executar o container MongoDB e o script Python, utilize:
```bash
.\run.ps1
```

Ou, em sistemas Unix:
```bash
./build.sh && ./run.sh
```

## Uso

Para executar o script manualmente, certifique-se de que o MongoDB esteja em execução e depois execute:

```bash
python api/api_client.py
```

O script irá buscar dados de opções da Binance a partir da data inicial especificada e armazená-los no banco de dados MongoDB.

### Uso do CSV

Além da coleta de dados da API da Binance, o projeto permite importar dados de um arquivo CSV. Para isso, certifique-se de que o arquivo `dados_completos.csv` esteja no caminho correto.

- O script irá detectar automaticamente o arquivo CSV e carregar os dados no MongoDB.
- Caso o arquivo CSV seja encontrado, ele será lido e os dados serão inseridos na coleção `options_chain` do banco de dados `binance_data`.
- O script está preparado para lidar com possíveis erros no formato do CSV, como problemas na conversão de datas ou ausência de colunas esperadas, e registrará esses erros no log.
- Também é possível acessar o CSV completo e agregados através da API Flask integrada.

Para executar o script com o CSV:
```bash
python api/api_client.py
```

Certifique-se de que o arquivo CSV esteja disponível na raiz do projeto ou atualize o caminho no script, se necessário.

## Estrutura do Projeto

- `api/api_client.py`: Script principal que coleta e salva os dados.
- `requirements.txt`: Lista de dependências do projeto.
- `.env`: Arquivo contendo variáveis de ambiente (não incluído no repositório).
- `build.ps1`: Script PowerShell para construir a imagem Docker do MongoDB.
- `build.sh`: Script PowerShell para construir a imagem Docker do MongoDB em sistemas Unix.
- `run.ps1`: Script PowerShell para executar o container MongoDB e o script Python.
- `run.sh`: Script Bash para executar o container MongoDB e o script Python em sistemas Unix.

## Observações

- Certifique-se de configurar o `MONGO_URI` corretamente, especialmente se estiver usando credenciais personalizadas.
- A chave da API deve ser válida para acessar os dados da Binance.
- Respeite os limites de taxa da API da Binance, fazendo ajustes no tempo de espera (`time.sleep`) se necessário.
- Caso utilize o CSV, verifique se ele está no formato esperado para evitar erros na inserção dos dados.
- A API Flask pode ser utilizada para baixar o CSV completo, acessando o endpoint `/api/dados_completos`.

## Problemas Comuns

- **Erro de Autenticação**: Verifique se a chave da API está correta e se tem as permissões necessárias.
- **Conexão com MongoDB**: Verifique se o MongoDB está em execução e se a URI está correta.
- **Problemas com o CSV**: Certifique-se de que o arquivo CSV está no local correto e que possui as colunas esperadas, como `time` e `price`.

## Licença

Este projeto está sob a licença MIT.

