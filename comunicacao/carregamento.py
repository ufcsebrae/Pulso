# comunicacao/carregamento.py
import logging
import pandas as pd
import numpy as np
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError

logger = logging.getLogger(__name__)

# Define o tamanho de cada lote. 10,000 é um bom valor inicial.
CHUNK_SIZE = 10000

# --------------------------------------------------------------------
# NOVA FUNÇÃO - Carga Inteligente que Evita Duplicatas
# --------------------------------------------------------------------
def carregar_dataframe_para_sql_sem_duplicados(
    df: pd.DataFrame,
    nome_tabela: str,
    engine: Engine,
    chave_primaria: list[str]
) -> None:
    """
    Carrega um DataFrame para uma tabela SQL, evitando a inserção de registros duplicados
    com base em uma chave primária (simples ou composta).
    """
    if df.empty:
        logger.warning(f"O DataFrame para a tabela '{nome_tabela}' está vazio. Nenhum dado será carregado.")
        return

    if not all(col in df.columns for col in chave_primaria):
        cols_faltando = [col for col in chave_primaria if col not in df.columns]
        error_msg = f"Colunas da chave primária não encontradas no DataFrame: {cols_faltando}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    df_para_inserir = df.copy()

    try:
        colunas_chave_str = ', '.join(f'"{col}"' for col in chave_primaria) # Adiciona aspas para nomes de colunas com caracteres especiais
        query = f"SELECT {colunas_chave_str} FROM {nome_tabela}"
        df_existente = pd.read_sql(query, engine)

        if not df_existente.empty:
            logger.info(f"Encontrados {len(df_existente)} registros existentes em '{nome_tabela}'. Filtrando para carregar apenas dados novos.")
            
            for col in chave_primaria:
                df_existente[col] = df_existente[col].astype(df_para_inserir[col].dtype)

            merged_df = df_para_inserir.merge(df_existente, on=chave_primaria, how='left', indicator=True)
            df_para_inserir = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])

    except ProgrammingError as e:
        if "does not exist" in str(e).lower() or "invalid object name" in str(e).lower():
            logger.info(f"A tabela '{nome_tabela}' não existe. Todos os {len(df_para_inserir)} registros serão carregados.")
        else:
            logger.exception(f"ERRO ao consultar dados existentes em '{nome_tabela}'.")
            raise
    
    if df_para_inserir.empty:
        logger.info(f"Nenhum registro novo para carregar na tabela '{nome_tabela}'. Carga finalizada.")
        return

    logger.info(f"Iniciando carga de {len(df_para_inserir)} novas linhas para a tabela '{nome_tabela}'...")

    try:
        df_para_inserir.to_sql(
            name=nome_tabela,
            con=engine,
            if_exists='append',
            index=False,
            chunksize=CHUNK_SIZE
        )
        logger.info(f"Carga de {len(df_para_inserir)} novos registros para '{nome_tabela}' concluída com sucesso!")
    except Exception as e:
        logger.exception(f"ERRO AO CARREGAR NOVOS DADOS PARA O SQL NA TABELA '{nome_tabela}'")
        raise

# --------------------------------------------------------------------
# FUNÇÃO ANTIGA - Mantida por segurança durante a transição
# --------------------------------------------------------------------
def carregar_dataframe_para_sql(df: pd.DataFrame, nome_tabela: str, engine: Engine) -> None:
    if df.empty:
        logger.warning("O DataFrame para a tabela '%s' está vazio. Nenhum dado será carregado.", nome_tabela)
        return
    logger.info("Iniciando carga de %d linhas para a tabela '%s' em lotes...", len(df), nome_tabela)
    num_chunks = int(np.ceil(len(df) / CHUNK_SIZE))
    try:
        for i, start in enumerate(range(0, len(df), CHUNK_SIZE)):
            end = start + CHUNK_SIZE
            df_chunk = df.iloc[start:end]
            if_exists_mode = 'replace' if i == 0 else 'append'
            logger.info(f"Carregando lote {i + 1}/{num_chunks} para '{nome_tabela}' (modo: {if_exists_mode})...")
            df_chunk.to_sql(name=nome_tabela, con=engine, if_exists=if_exists_mode, index=False)
        logger.info("Carga em lotes para a tabela '%s' concluída com sucesso!", nome_tabela)
    except Exception as e:
        logger.exception("ERRO AO CARREGAR DADOS EM LOTE PARA O SQL NA TABELA '%s'", nome_tabela)
        raise e
