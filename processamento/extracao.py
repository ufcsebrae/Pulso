# processamento/extracao.py (VERSÃO COMPLETA E CORRIGIDA)
import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy.engine import Engine

from config.config import CONFIG
from config.database import get_conexao
from utils.utils import carregar_script_sql

logger = logging.getLogger(__name__)

# Constantes para os nomes das tabelas no cache
TABELA_ORCADO_CACHE = "orcado_nacional_raw"
TABELA_CC_CACHE = "cc_estrutura_raw"


def obter_dados_brutos() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Obtém os DataFrames BRUTOS do Orçado e da Estrutura de CC, otimizando
    a criação de conexões de cache.
    """
    caminho_cache: Path = CONFIG.paths.cache_db

    if not caminho_cache.exists():
        logger.warning("Arquivo de cache '%s' não encontrado. Executando queries ao vivo...", caminho_cache.name)
        df_orcado = _buscar_dados_financa_sql_raw()
        df_cc = _buscar_dados_hubdados_sql_raw()
        logger.info("Salvando dados brutos no cache local...")
        engine_cache = get_conexao(CONFIG.conexoes["CacheDB"])
        _salvar_dados_no_cache(df_orcado, df_cc, engine_cache)
    else:
        logger.info("Carregando dados brutos do cache local '%s'...", caminho_cache.name)
        try:
            engine_cache = get_conexao(CONFIG.conexoes["CacheDB"])
            df_orcado = _carregar_dados_do_cache(TABELA_ORCADO_CACHE, engine_cache)
            df_cc = _carregar_dados_do_cache(TABELA_CC_CACHE, engine_cache)
            logger.info("Dados brutos carregados do cache com sucesso.")
        except Exception as e:
            logger.error("Erro ao ler tabelas do cache: %s. O cache pode estar corrompido.", e)
            logger.warning("Excluindo cache e tentando buscar dados ao vivo.")
            caminho_cache.unlink()
            return obter_dados_brutos()
    return df_orcado, df_cc


def _buscar_dados_financa_sql_raw() -> pd.DataFrame:
    """
    Busca dados brutos do Orçado (Nacional) via SQL Server FINANCA.
    """
    logger.info("Buscando dados brutos do Orçado (Nacional) via SQL Server...")
    query = carregar_script_sql(CONFIG.paths.query_nacional)
    engine = get_conexao(CONFIG.conexoes["FINANCA_SQL"])
    try:
        df = pd.read_sql(query, engine)
        logger.info("Dados do Orçado (SQL) carregados com sucesso (%d linhas).", len(df))
        return df
    except Exception as e:
        logger.exception("ERRO CRÍTICO AO BUSCAR DADOS DO SQL FINANCA (ORÇADO).")
        raise e


def _buscar_dados_hubdados_sql_raw() -> pd.DataFrame:
    """Busca dados brutos da estrutura de CC."""
    logger.info("Buscando dados brutos da estrutura de CC...")
    query = carregar_script_sql(CONFIG.paths.query_cc)
    engine = get_conexao(CONFIG.conexoes["HubDados"])
    return pd.read_sql(query, engine)


def _salvar_dados_no_cache(df_orcado: pd.DataFrame, df_cc: pd.DataFrame, engine_cache: Engine) -> None:
    """Salva os DataFrames brutos no cache SQLite."""
    df_orcado.to_sql(TABELA_ORCADO_CACHE, engine_cache, if_exists="replace", index=False)
    df_cc.to_sql(TABELA_CC_CACHE, engine_cache, if_exists="replace", index=False)
    logger.info("Cache de dados brutos criado com sucesso.")


def _carregar_dados_do_cache(tabela: str, engine_cache: Engine) -> pd.DataFrame:
    """Carrega uma tabela específica do cache SQLite usando uma conexão existente."""
    return pd.read_sql(tabela, engine_cache)


# --- FUNÇÃO ADICIONADA PARA CORRIGIR O ERRO ---
def obter_dados_comprometidos_brutos() -> pd.DataFrame:
    """
    Busca dados brutos do Comprometido (Nacional) via SQL Server FINANCA.
    """
    logger.info("Buscando dados brutos do Comprometido (Nacional) via SQL Server...")
    try:
        # Assumimos que a query principal do comprometido está em 'comprometido.sql'
        query_path = CONFIG.paths.queries_dir / "comprometido.sql"
        query = carregar_script_sql(query_path)
    except FileNotFoundError as e:
        logger.error(f"Arquivo de query 'comprometido.sql' não foi encontrado. {e}")
        # Retornar um DataFrame vazio é mais seguro do que quebrar a execução
        return pd.DataFrame()

    engine = get_conexao(CONFIG.conexoes["FINANCA_SQL"])
    
    try:
        df = pd.read_sql(query, engine)
        logger.info("Dados do Comprometido (SQL) carregados com sucesso (%d linhas).", len(df))
        return df
    except Exception as e:
        logger.exception("ERRO CRÍTICO AO BUSCAR DADOS DO COMPROMETIDO.")
        raise e


def obter_dados_correlacao(nome_query: str, centros_de_custo: list[str], truncate_cc_keys: bool = False) -> pd.DataFrame | None:
    """
    Busca dados de correlação, com opção de truncar as chaves de CC para correspondência.
    """
    logger.info(f"Buscando dados de correlação da query '{nome_query}' para {len(centros_de_custo)} centros de custo.")
    if not centros_de_custo:
        logger.warning("Lista de centros de custo está vazia. Nenhum dado de correlação será buscado.")
        return None
    try:
        caminho_query = CONFIG.paths.queries_dir / nome_query
        query_sql = carregar_script_sql(caminho_query)
        engine = get_conexao(CONFIG.conexoes["FINANCA_SQL"])
        df_full = pd.read_sql(query_sql, engine)
        logger.info(f"Query '{nome_query}' retornou {len(df_full)} linhas.")
        if df_full.empty: return df_full
        cc_col = 'CODCCUSTO' if 'CODCCUSTO' in df_full.columns else 'CC'
        if cc_col not in df_full.columns:
            logger.error(f"Query '{nome_query}' não contém coluna de Centro de Custo ('CODCCUSTO' ou 'CC').")
            return None
        df_filtered = df_full.copy()
        centros_de_custo_para_filtro = centros_de_custo
        if truncate_cc_keys:
            logger.info("Aplicando lógica de truncagem de CCs para correspondência.")
            centros_de_custo_para_filtro = list(set(['.'.join(str(cc).split('.')[:-1]) for cc in centros_de_custo if '.' in str(cc)]))
        df_filtered[cc_col] = df_filtered[cc_col].astype(str).str.strip()
        centros_de_custo_str = [str(cc).strip() for cc in centros_de_custo_para_filtro]
        df_filtered = df_filtered[df_filtered[cc_col].isin(centros_de_custo_str)]
        logger.info(f"Filtro de Centro de Custo resultou em {len(df_filtered)} linhas.")
        if 'ANO' in df_filtered.columns:
            ano_filtro = int(os.getenv("ANO_FILTRO", 2025))
            df_filtered['ANO'] = pd.to_numeric(df_filtered['ANO'], errors='coerce')
            df_filtered = df_filtered[df_filtered['ANO'] == ano_filtro]
            logger.info(f"Filtro final por ANO={ano_filtro} resultou em {len(df_filtered)} linhas.")
        else:
            logger.warning(f"Coluna 'ANO' não encontrada em '{nome_query}'. A filtragem por ano será pulada.")
        return df_filtered
    except Exception as e:
        logger.exception(f"Falha ao buscar e filtrar dados de correlação da query '{nome_query}': {e}")
        return None
