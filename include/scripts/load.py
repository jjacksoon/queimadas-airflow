import pandas as pd 
from sqlalchemy import create_engine, text
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
            
        # --- LÓGICA DE IDEMPOTÊNCIA ---
        # 4. Extrair a data que estamos carregando (baseada na primeira linha da coluna data_hora_gmt)
        # Convertemos para o formato de data do SQL (YYYY-MM-DD)
        data_carga = pd.to_datetime(df['data_hora_gmt']).dt.date.iloc[0]
        
        print(f"Limpando dados antigos da data: {data_carga} para evitar duplicados...")
        
        with engine.connect() as conn:
            # Comando SQL para deletar registros existentes daquela data
            # Usamos DATE() para comparar apenas o dia, ignorando horas/minutos
            query = text(f"DELETE FROM focos_queimadas WHERE DATE(data_hora_gmt) = '{data_carga}'")
            conn.execute(query)
            conn.commit() # Confirma a exclusão


        # 4. Inserindo dados no banco
        # 'focos_queimadas' será o nme da tabela
        # if_exists = append garante que os dados novos sejam adicionados aos antigos
        df.to_sql('focos_queimadas', con=engine, if_exists='append', index=False)

        print(f"Sucesso! {len(df)} linhas carregadas na tabela 'focos_queimadas'")
    except Exception as e:
        print(f"Erro ao carregar os dados no banco: {e}")
        raise e