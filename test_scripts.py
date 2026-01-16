from include.scripts.extract import extract_queimadas
from include.scripts.transform import transform_queimadas
from include.scripts.load import load_queimadas

def testar_pipeline_local():
    print("--- INICIANDO TESTE ---")

    try:
        #Extração
        print(f"\n Passo 1: Extração...")
        caminho_bruto = extract_queimadas()
        print(f"Arquivo bruto gerado em: {caminho_bruto}")

        #Transformação
        print(f"\n Passo 2: Transformação...")
        caminho_limpo = transform_queimadas(caminho_bruto)
        print(f"Arquivo limpo gerado em: {caminho_limpo}")

        #Carregamento
        print(f"\n Passo 3: Carregamento no banco de dados...")
        load_queimadas(caminho_limpo)

        print("--- TESTE CONCLUIDO COM SUCESSO ---")

    except Exception as e:
        print("\n--- O TESTE FALHOU! ---")
        print(f"Erro encontrado: {e}")

if __name__ == "__main__":
    testar_pipeline_local()