import logging
import pandas as pd
import sqlite3
import sqlalchemy # Necessário para os dtypes

# Configuração do Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Importe suas configurações de projeto
from config.config import CONFIG
from config.database import get_conexao

# --- PONTO DE ATENÇÃO: CONFIRME ESTE NOME ---
# Este deve ser o nome da tabela DENTRO do 'local_cache.db' que contém o resultado
# final, já enriquecido, de uma execução anterior. É o nome que seu script
# usa quando salva o resultado no cache. Um nome provável é 'orcado_enriquecido_com_cc'.
NOME_TABELA_ENRIQUECIDA_NO_CACHE = 'orcado_enriquecido_com_cc'

CAMINHO_SQLITE_DB = "cache/local_cache.db"

def rebuild_and_migrate_map():
    """
    Reconstrói o mapa de correções a partir da tabela de resultado final
    armazenada no cache SQLite e o migra para o SQL Server.
    """
    logging.info("--- INICIANDO RECONSTRUÇÃO E MIGRAÇÃO DO MAPA DE CORREÇÕES ---")
    
    conn = None
    try:
        # 1. LER O RESULTADO FINAL DO CACHE SQLITE
        logging.info(f"Conectando ao cache em '{CAMINHO_SQLITE_DB}'...")
        conn = sqlite3.connect(CAMINHO_SQLITE_DB)
        
        logging.info(f"Lendo a tabela de resultado final '{NOME_TABELA_ENRIQUECIDA_NO_CACHE}'...")
        
        # Seleciona as duas colunas que formam o "mapa"
        query = f"SELECT CHAVE_CONCAT_original, CHAVE_CONCAT FROM {NOME_TABELA_ENRIQUECIDA_NO_CACHE}"
        df = pd.read_sql_query(query, conn)
        
        logging.info(f"{len(df)} linhas lidas do cache.")

    except (sqlite3.Error, pd.errors.DatabaseError) as e:
        logging.error(f"!!! ERRO CRÍTICO ao ler o cache SQLite: {e}")
        logging.error("Verifique se o nome da tabela '%s' está correto e tente novamente.", NOME_TABELA_ENRIQUECIDA_NO_CACHE)
        return
    finally:
        if conn:
            conn.close()

    # 2. RECONSTRUIR O MAPA DE CORREÇÕES
    logging.info("Reconstruindo o mapa a partir das chaves originais e corrigidas...")
    
    # Filtra apenas as linhas onde a correção de fato aconteceu
    df_correcoes = df[df['CHAVE_CONCAT_original'] != df['CHAVE_CONCAT']].copy()
    
    # Garante que não há pares duplicados
    df_correcoes = df_correcoes[['CHAVE_CONCAT_original', 'CHAVE_CONCAT']].drop_duplicates()
    
    if df_correcoes.empty:
        logging.warning("Nenhuma diferença encontrada entre chaves originais e corrigidas. Nenhuma correção para migrar.")
        return
        
    logging.info(f"Sucesso! {len(df_correcoes)} correções únicas reconstruídas a partir do cache.")
    
    # Renomeia as colunas para corresponder à tabela do SQL Server
    df_correcoes.rename(columns={
        'CHAVE_CONCAT_original': 'ChaveQuebrada',
        'CHAVE_CONCAT': 'ChaveCorreta'
    }, inplace=True)

    # 3. MIGRAR O MAPA RECONSTRUÍDO PARA O SQL SERVER
    try:
        engine_financa = get_conexao(CONFIG.conexoes["FINANCA_SQL"])
        
        logging.warning("Conectando ao SQL Server para substituir a tabela 'MapaCorrecoesChaves'...")
        
        df_correcoes.to_sql(
            name='MapaCorrecoesChaves',
            con=engine_financa,
            if_exists='replace', # Substitui completamente a tabela
            index=False,
            dtype={
                'ChaveQuebrada': sqlalchemy.types.NVARCHAR(255),
                'ChaveCorreta': sqlalchemy.types.NVARCHAR(255)
            }
        )
        
        logging.info("--- MIGRAÇÃO COMPLETA ---")
        logging.info(f"As {len(df_correcoes)} correções foram salvas com sucesso na tabela 'MapaCorrecoesChaves' do SQL Server.")
        
    except Exception as e:
        logging.exception("--- FALHA CRÍTICA DURANTE A MIGRAÇÃO PARA O SQL ---")
        raise

if __name__ == "__main__":
    rebuild_and_migrate_map()
