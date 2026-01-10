# comunicacao/carregamento.py
import logging
import pandas as pd
import numpy as np
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Define o tamanho de cada lote. 10,000 é um bom valor inicial.
CHUNK_SIZE = 10000

def carregar_dataframe_para_sql(df: pd.DataFrame, nome_tabela: str, engine: Engine) -> None:
    """
    Carrega um DataFrame para uma tabela SQL em lotes (chunks), com transações
    separadas para cada lote, evitando timeouts.

    - O primeiro lote substitui a tabela (if_exists='replace').
    - Os lotes subsequentes anexam os dados (if_exists='append').
    - O progresso é logado no console a cada lote.
    """
    if df.empty:
        logger.warning("O DataFrame para a tabela '%s' está vazio. Nenhum dado será carregado.", nome_tabela)
        return

    logger.info(
        "Iniciando carga de %d linhas para a tabela '%s' em lotes de até %d linhas...",
        len(df), nome_tabela, CHUNK_SIZE
    )

    # Calcula o número total de lotes
    num_chunks = int(np.ceil(len(df) / CHUNK_SIZE))

    try:
        # Loop através dos lotes
        for i, start in enumerate(range(0, len(df), CHUNK_SIZE)):
            end = start + CHUNK_SIZE
            df_chunk = df.iloc[start:end]

            # Determina o modo de gravação: 'replace' para o primeiro, 'append' para os outros
            if_exists_mode = 'replace' if i == 0 else 'append'
            
            # Loga o progresso no console
            logger.info(
                f"Carregando lote {i + 1}/{num_chunks} ({len(df_chunk)} linhas) para a tabela '{nome_tabela}' (modo: {if_exists_mode})..."
            )

            # Envia o lote para o banco de dados
            df_chunk.to_sql(
                name=nome_tabela,
                con=engine,
                if_exists=if_exists_mode,
                index=False,
                # O chunksize interno não é necessário, pois já estamos fazendo manualmente
            )
        
        logger.info("Carga em lotes para a tabela '%s' concluída com sucesso!", nome_tabela)

    except Exception as e:
        logger.exception("ERRO AO CARREGAR DADOS EM LOTE PARA O SQL NA TABELA '%s'", nome_tabela)
        raise e

