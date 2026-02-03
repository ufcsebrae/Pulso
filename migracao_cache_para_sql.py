import json
import logging
import pandas as pd
import sqlalchemy # Necessário para os dtypes

# Configuração básica para vermos o que está acontecendo
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Importe suas configurações de conexão
from config.config import CONFIG
from config.database import get_conexao

def run_one_time_migration():
    """
    Lê o mapa de correções de um arquivo JSON local e o carrega em massa
    para a tabela MapaCorrecoesChaves no SQL Server.
    
    ATENÇÃO: Substitui completamente os dados na tabela de destino!
    """
    logging.info("--- INICIANDO MIGRAÇÃO DO CACHE LOCAL PARA O SQL SERVER ---")
    
    # --- ALTERE AQUI O CAMINHO PARA SEU ARQUIVO DE CACHE ---
    # Coloque o caminho exato para o arquivo que contém suas correções.
    # Geralmente é algo como 'cache/mapa_correcoes.json'
    # Se você não o moveu, ele pode estar na raiz ou em uma pasta 'cache'.
    CAMINHO_ARQUIVO_CACHE = "dados/mapa_correcoes.json" # <<< CONFIRME ESTE CAMINHO
    
    try:
        with open(CAMINHO_ARQUIVO_CACHE, 'r', encoding='utf-8') as f:
            mapa_local = json.load(f)
        logging.info(f"Arquivo de cache '{CAMINHO_ARQUIVO_CACHE}' lido com sucesso.")
    except FileNotFoundError:
        logging.error(f"!!! ERRO CRÍTICO: O arquivo de cache '{CAMINHO_ARQUIVO_CACHE}' não foi encontrado.")
        logging.error("A migração não pode continuar. Verifique o caminho e tente novamente.")
        return
    except json.JSONDecodeError:
        logging.error(f"!!! ERRO CRÍTICO: O arquivo '{CAMINHO_ARQUIVO_CACHE}' parece estar corrompido.")
        return

    if not mapa_local:
        logging.warning("O arquivo de cache está vazio. Nenhuma correção para migrar.")
        return

    df_correcoes = pd.DataFrame(
        list(mapa_local.items()),
        columns=['ChaveQuebrada', 'ChaveCorreta']
    )
    
    logging.info(f"Encontradas {len(df_correcoes)} correções no arquivo local. Preparando para enviar ao SQL Server...")

    try:
        engine_financa = get_conexao(CONFIG.conexoes["FINANCA_SQL"])
        
        logging.warning("Conectando ao SQL... A tabela 'MapaCorrecoesChaves' será substituída.")
        
        df_correcoes.to_sql(
            name='MapaCorrecoesChaves',
            con=engine_financa,
            if_exists='replace', # Apaga tudo o que existe lá e insere esta nova carga
            index=False,
            dtype={ # Garante que os tipos de texto sejam compatíveis com o SQL Server
                'ChaveQuebrada': sqlalchemy.types.NVARCHAR(255),
                'ChaveCorreta': sqlalchemy.types.NVARCHAR(255)
            }
        )
        
        logging.info("--- SUCESSO! ---")
        logging.info(f"As {len(df_correcoes)} correções foram migradas para a tabela 'MapaCorrecoesChaves'.")
        
    except Exception as e:
        logging.exception("--- FALHA CRÍTICA DURANTE A MIGRAÇÃO PARA O SQL ---")
        raise

if __name__ == "__main__":
    run_one_time_migration()
