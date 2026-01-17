import pandas as pd
import os
from datetime import datetime

def transform_queimadas(input_path):
    print(f"Iniciando transformação do arquivo:{input_path}")

    # 1. Lendo CSV bruto
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {input_path}")
        
    df = pd.read_csv(input_path)

    # 2. Selecionando apenas colunas essenciais
    colunas_essenciais = [
        "id",
        "data_hora_gmt",
        "satelite",
        "municipio",
        "estado",
        "numero_dias_sem_chuva",
        "risco_fogo",
        "bioma"
    ]

    df_clean = df[colunas_essenciais].copy()

    # 3. Tratamento de tipagem (Data Quality) da informação

    # Tratando valores vazios
    df_clean['numero_dias_sem_chuva'] = df_clean['numero_dias_sem_chuva'].fillna(0)
    df_clean['risco_fogo'] = df_clean['risco_fogo'].fillna(0)

    # Converter a coluna data_hora de string para o objeto datetime do Python
    df_clean['data_hora_gmt'] = pd.to_datetime(df_clean['data_hora_gmt'], errors='coerce')

    # 4. Definindo caminho do arquivo de saida (camada silver)
    hoje = datetime.now().strftime('%Y%m%d')
    output_path = f"include/data/silver/focos_clean_{hoje}.csv"
    
    # Garantindo que a pasta exista
    os.makedirs('include/data/silver', exist_ok=True)

    # 5. Salvar sem o índice do Pandas (coluna extra de números)
    df_clean.to_csv(output_path, index=False)
    
    print(f"Transformação concluída! Dados limpos em: {output_path}")
    return output_path