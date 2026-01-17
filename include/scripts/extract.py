import pandas as pd
import os
import requests
from datetime import datetime

def extract_queimadas():
    # 1. Gerar url dinânmica uma vez que o arquivo extraido muda diariamente
    hoje = datetime.now().strftime('%Y%m%d')
    # A URL precisa terminar com o nome do arquivo .csv
    url = f"https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/diario/Brasil/focos_diario_br_{hoje}.csv"

    #Caminho absoluto da pasta
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    #Destino camada bronze
    raw_dir = os.path.join(base_path, 'data', 'raw')

    # Garantindo que a pasta exista
    os.makedirs(raw_dir, exist_ok=True)

    raw_data_path = os.path.join(raw_dir, f"focos_raw_{hoje}.csv")

    try:
        print(f"Baixando dados do INPE: {url}")

        #Requisição HTTP
        response = requests.get(url, timeout = 30)
        response.raise_for_status()

        #Escrevendo o conteudo no arquivo local
        with open(raw_data_path, 'wb') as f:
            f.write(response.content)
        
        print(f"Extração concluida. Arquivo salvo em: {raw_data_path}")

        #Retornando o caminho para que Airflow possa passar para a próxima tarefa
        return raw_data_path
    
    except Exception as e:
        print(f"Erro na extração: {e}")
        raise e
