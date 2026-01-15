import pandas as pd
import os
import requests
from datetime import datetime

def extract_queimadas():
    # 1. Gerar url dinânmica uma vez que o arquivo extraido muda diariamente
    hoje = datetime.now().strftime('%Y%m%d')
    url = 'https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/diario/Brasil/'

    #Destino camada bronze
    raw_data_path = f"include/data/focos_raw_{hoje}.csv"

    # Garantindo que a pasta exista
    os.makedirs('include/data', exist_ok = True)

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
    
    exception Exception as e:
        print(f"Erro na extração: {e}")
        raise e
