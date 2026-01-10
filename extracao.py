# extracao.py
import logging
import pandas as pd
from pathlib import Path

# Importações do projeto
from config import CONFIG
from database import get_conexao
from utils import carregar_script_sql

logger = logging.getLogger(__name__)

# Constantes para os nomes das tabelas no cache, para evitar erros de digitação.
# Usamos "_raw" para deixar claro que estamos armazenando os dados brutos.
TABELA_ORCADO_CACHE = "orcado_nacional_raw"
TABELA_CC_CACHE = "cc_estrutura_raw"

def obter_dados_brutos() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Obtém os DataFrames BRUTOS do Orçado e da Estrutura de CC.

    A lógica principal é:
    1. Verifica se o arquivo de cache (cache_dados.db) existe.
    2. Se NÃO existir, executa as queries nas fontes de dados ao vivo,
       cria o arquivo de cache e salva os dados brutos nele.
    3. Se existir, carrega os dados diretamente do cache, o que é muito mais rápido.
    4. Inclui uma proteção: se houver um erro ao ler o cache (arquivo corrompido),
       ele apaga o cache e tenta o processo novamente do zero.

    Returns:
        Uma tupla contendo os dois DataFrames brutos: (df_orcado, df_cc).
    """
    caminho_cache: Path = CONFIG.paths.cache_db

    # 1. Condicional: "se o db não existir..."
    if not caminho_cache.exists():
        logger.warning("Arquivo de cache '%s' não encontrado. Executando queries ao vivo...", caminho_cache.name)
        
        # "...gere novamente"
        df_orcado = _buscar_dados_financa_sql_raw()
        df_cc = _buscar_dados_hubdados_sql_raw()
        _salvar_dados_no_cache(df_orcado, df_cc)
    
    # 2. Condicional: "se o db existir..."
    else:
        logger.info("Carregando dados brutos do cache local '%s'...", caminho_cache.name)
        
        # "...use ele"
        try:
            df_orcado = _carregar_dados_do_cache(TABELA_ORCADO_CACHE)
            df_cc = _carregar_dados_do_cache(TABELA_CC_CACHE)
            logger.info("Dados brutos carregados do cache com sucesso.")
        except Exception as e:
            # Lógica de segurança para cache corrompido
            logger.error("Erro ao ler tabelas do cache: %s. O cache pode estar corrompido.", e)
            logger.warning("Excluindo cache e tentando buscar dados ao vivo.")
            caminho_cache.unlink() # Deleta o arquivo problemático
            return obter_dados_brutos() # Tenta executar a função novamente do zero

    return df_orcado, df_cc

def _buscar_dados_financa_sql_raw() -> pd.DataFrame:
    """Função privada para buscar dados brutos do Orçado (Nacional)."""
    logger.info("Buscando dados brutos do Orçado (Nacional)...")
    query = carregar_script_sql(CONFIG.paths.query_nacional)
    engine_config = CONFIG.conexoes["FINANCA_SQL"]
    engine = get_conexao(engine_config)
    return pd.read_sql(query, engine)

def _buscar_dados_hubdados_sql_raw() -> pd.DataFrame:
    """Função privada para buscar dados brutos da estrutura de CC."""
    logger.info("Buscando dados brutos da estrutura de CC...")
    query = carregar_script_sql(CONFIG.paths.query_cc)
    engine_config = CONFIG.conexoes["HubDados"]
    engine = get_conexao(engine_config)
    return pd.read_sql(query, engine)

def _salvar_dados_no_cache(df_orcado: pd.DataFrame, df_cc: pd.DataFrame) -> None:
    """Função privada para salvar os DataFrames brutos no cache SQLite."""
    logger.info("Salvando dados brutos no cache local...")
    engine_cache_config = CONFIG.conexoes["CacheDB"]
    engine_cache = get_conexao(engine_cache_config)
    
    df_orcado.to_sql(TABELA_ORCADO_CACHE, engine_cache, if_exists="replace", index=False)
    df_cc.to_sql(TABELA_CC_CACHE, engine_cache, if_exists="replace", index=False)
    
    logger.info("Cache de dados brutos criado com sucesso.")

def _carregar_dados_do_cache(tabela: str) -> pd.DataFrame:
    """Função privada para carregar uma tabela específica do cache SQLite."""
    engine_cache_config = CONFIG.conexoes["CacheDB"]
    engine_cache = get_conexao(engine_cache_config)
    return pd.read_sql(tabela, engine_cache)
