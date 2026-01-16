import pandas as pd 
from sqlalchemy import create_engine
import os

def load_queimadas(input_path):
    print(F"--- INICIANDO CARGA ---")

    # 1. Configurando conexão com o Postgres do Astro CLI
    # host 'postgres' é o nome do serviço dentro da rede do Docker
    conn_string = "postgresql://postgres:postgres@localhost:5432/postgres"

    try:
        # 2. Criando o motor de conexão
        engine = create_engine(conn_string)

        # 3. Lendo os dados da silver
        print(f"Lendo dados de: {input_path}")
        df = pd.read_csv(input_path)

        if df.empty:
            print("O Dataframe está vazio. Nenhuma carga será realizada")
            return
        
        # 4. Inserindo dados no banco
        # 'focos_queimadas' será o nme da tabela
        # if_exists = append garante que os dados novos sejam adicionados aos antigos
        df.to_sql('focos_queimadas', con=engine, if_exists='append', index=False)

        print(f"Sucesso! {len(df)} linhas carregadas na tabela 'focos_queimadas'")
    except Exception as e:
        print(f"Erro ao carregar os dados no banco: {e}")
        raise e