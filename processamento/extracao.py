# processamento/extracao.py (VERSÃO FINAL COM LÓGICA DE TRUNCAGEM)
import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy.engine import Engine

from config.config import CONFIG
from config.database import get_conexao
from utils.utils import carregar_script_sql

logger = logging.getLogger(__name__)

# ... (o resto do arquivo, como obter_dados_brutos, permanece o mesmo) ...

def obter_dados_brutos() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Obtém os DataFrames BRUTOS do Orçado e da Estrutura de CC, otimizando
    a criação de conexões de cache.
    """
    caminho_cache: Path = CONFIG.paths.cache_db

    if not caminho_cache.exists():
        logger.warning(
            "Arquivo de cache '%s' não encontrado. Executando queries ao vivo...",
            caminho_cache.name,
        )

        df_orcado = _buscar_dados_financa_sql_raw()
        df_cc = _buscar_dados_hubdados_sql_raw()
        
        logger.info("Salvando dados brutos no cache local...")
        engine_cache = get_conexao(CONFIG.conexoes["CacheDB"])
        _salvar_dados_no_cache(df_orcado, df_cc, engine_cache)

    else:
        logger.info(
            "Carregando dados brutos do cache local '%s'...", caminho_cache.name
        )
        try:
            engine_cache = get_conexao(CONFIG.conexoes["CacheDB"])
            
            df_orcado = _carregar_dados_do_cache("orcado_nacional_raw", engine_cache)
            df_cc = _carregar_dados_do_cache("cc_estrutura_raw", engine_cache)
            
            logger.info("Dados brutos carregados do cache com sucesso.")
        except Exception as e:
            logger.error(
                "Erro ao ler tabelas do cache: %s. O cache pode estar corrompido.", e
            )
            logger.warning("Excluindo cache e tentando buscar dados ao vivo.")
            caminho_cache.unlink()
            return obter_dados_brutos()

    return df_orcado, df_cc


def _buscar_dados_financa_sql_raw() -> pd.DataFrame:
    logger.info("Buscando dados brutos do Orçado (Nacional) via SQL Server...")
    query = carregar_script_sql(CONFIG.paths.query_nacional)
    engine = get_conexao(CONFIG.conexoes["FINANCA_SQL"])
    try:
        df = pd.read_sql(query, engine)
        logger.info("Dados do Orçado (SQL) carregados com sucesso (%d linhas).", len(df))
        return df
    except Exception as e:
        logger.exception("ERRO CRÍTICO AO BUSCAR DADOS DO SQL FINANCA.")
        raise e

def _buscar_dados_hubdados_sql_raw() -> pd.DataFrame:
    logger.info("Buscando dados brutos da estrutura de CC...")
    query = carregar_script_sql(CONFIG.paths.query_cc)
    engine = get_conexao(CONFIG.conexoes["HubDados"])
    return pd.read_sql(query, engine)

def _salvar_dados_no_cache(df_orcado: pd.DataFrame, df_cc: pd.DataFrame, engine_cache: Engine) -> None:
    df_orcado.to_sql("orcado_nacional_raw", engine_cache, if_exists="replace", index=False)
    df_cc.to_sql("cc_estrutura_raw", engine_cache, if_exists="replace", index=False)
    logger.info("Cache de dados brutos criado com sucesso.")

def _carregar_dados_do_cache(tabela: str, engine_cache: Engine) -> pd.DataFrame:
    return pd.read_sql(tabela, engine_cache)
    
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
        
        # Prepara a lista de CCs para o filtro
        centros_de_custo_para_filtro = centros_de_custo
        if truncate_cc_keys:
            logger.info("Aplicando lógica de truncagem de CCs para correspondência.")
            # Remove o último segmento de cada CC (ex: 'X.Y.Z' -> 'X.Y')
            centros_de_custo_para_filtro = list(set(['.'.join(str(cc).split('.')[:-1]) for cc in centros_de_custo if '.' in str(cc)]))
        
        # Filtra pelos Centros de Custo (seja a lista original ou a truncada)
        df_filtered[cc_col] = df_filtered[cc_col].astype(str).str.strip()
        centros_de_custo_str = [str(cc).strip() for cc in centros_de_custo_para_filtro]
        df_filtered = df_filtered[df_filtered[cc_col].isin(centros_de_custo_str)]

        logger.info(f"Filtro de Centro de Custo resultou em {len(df_filtered)} linhas.")
        
        # Filtra pelo ANO apenas se a coluna 'ANO' existir no resultado
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
