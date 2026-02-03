# comunicacao/carregamento.py
import logging
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError

logger = logging.getLogger(__name__)

CHUNK_SIZE = 10000

def carregar_dataframe_para_sql_sem_duplicados(
    df: pd.DataFrame,
    nome_tabela: str,
    engine: Engine,
    chave_primaria: list[str]
) -> None:
    """
    Carrega um DataFrame para o SQL, evitando duplicatas com base em uma chave primária.
    """
    if df.empty:
        logger.warning(f"DataFrame para '{nome_tabela}' está vazio. Carga ignorada.")
        return

    if not all(col in df.columns for col in chave_primaria):
        faltando = [col for col in chave_primaria if col not in df.columns]
        raise ValueError(f"Colunas da chave primária não encontradas no DataFrame: {faltando}")

    df_para_inserir = df.copy()

    try:
        colunas_chave_str = ', '.join(f'"{col}"' for col in chave_primaria)
        query = f"SELECT {colunas_chave_str} FROM {nome_tabela}"
        df_existente = pd.read_sql(query, engine)

        if not df_existente.empty:
            logger.info(f"Encontrados {len(df_existente)} registros em '{nome_tabela}'. Filtrando dados novos.")
            for col in chave_primaria:
                df_existente[col] = df_existente[col].astype(df_para_inserir[col].dtype)
            
            merged = df_para_inserir.merge(df_existente, on=chave_primaria, how='left', indicator=True)
            df_para_inserir = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

    except ProgrammingError as e:
        if "invalid object name" in str(e).lower() or "does not exist" in str(e).lower():
            logger.info(f"Tabela '{nome_tabela}' não existe. Todos os {len(df_para_inserir)} registros serão carregados.")
        else:
            raise

    if df_para_inserir.empty:
        logger.info(f"Nenhum registro novo para carregar em '{nome_tabela}'.")
        return

    logger.info(f"Iniciando carga de {len(df_para_inserir)} novas linhas para '{nome_tabela}'...")

    try:
        df_para_inserir.to_sql(name=nome_tabela, con=engine, if_exists='append', index=False, chunksize=CHUNK_SIZE)
        logger.info(f"Carga de {len(df_para_inserir)} novos registros para '{nome_tabela}' concluída.")
    except Exception:
        logger.exception(f"ERRO AO CARREGAR DADOS PARA O SQL NA TABELA '{nome_tabela}'")
        raise
