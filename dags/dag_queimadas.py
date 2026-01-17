from airflow.decorators import dag, task
from datetime import datetime
import sys
import os

# --- CONFIGURAÇÃO DE CAMINHO (PATH) ---
# Como a DAG está na pasta 'dags' e os scripts na pasta 'include', precisamos 
# dizer ao Python para subir um nível ('..') e entrar na 'include'. 
# Sem isso, o Airflow não encontraria os arquivos extract, transform e load.
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(root_path, 'include'))

# Importando as 3 funcções ETL
from scripts.extract import extract_queimadas
from scripts.transform import transform_queimadas
from scripts.load import load_queimadas

# --- DEFINIÇÃO DA ESTRUTURA DA DAG ---
@dag(
    dag_id='etl_queimadas_inpe_medalhao',   # Identificador único que aparece no painel do Airflow
    schedule='30 13 * * *',                  # Agendamento: roda automaticamente uma vez ao dia -> 0 minutos, 13 horas (UTC), todos os dias que equivale a 10h da manhã horário local

    start_date=datetime(2026,1,16),       # Data a partir da qual o Airflow começa a contar o tempo
    catchup=False                           # Não rode os dias passados.Comece hoje
)

def pipeline_queimadas():
    # --- DEFININDO TAREFAS (TASKS) ---
    @task       # Unidade de trabalho de extração
    def extrair():
        return extract_queimadas()

    @task       # Unidade de trabalho de transformação
    def transformar(caminho_raw):
        return transform_queimadas(caminho_raw)

    @task       # Unidade de trabalho de carregamento
    def carregar(caminho_silver):
        load_queimadas(caminho_silver)

    # --- FLUXO DAS ATIVIDADES ---
    # 1. Executa a extração e guarda o caminho do arquivo na variável 'caminho_bruto'.
    caminho_bruto = extrair()        
    # 2. Passa o 'caminho_bruto' para a transformação. O Airflow garante que 
    # esta tarefa só comece após a extração terminar com sucesso.
    caminho_limpo = transformar(caminho_bruto)        
    # 3. Passa o 'caminho_limpo' para a carga final no banco de dados.
    carregar(caminho_limpo)

# --- INVOCAÇÃO ---
# Registra a DAG no sistema do Airflow para que ela apareça na interface Web.
pipeline_queimadas()