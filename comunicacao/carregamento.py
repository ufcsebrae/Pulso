# comunicacao/carregamento.py (VERSÃO FINAL COM CRIAÇÃO DINÂMICA DE TABELA)
import logging
import pandas as pd
from sqlalchemy.engine import Engine, reflection
from sqlalchemy import text

logger = logging.getLogger(__name__)

def carregar_dataframe_para_sql_com_merge(
    df: pd.DataFrame,
    nome_tabela_final: str,
    engine: Engine,
    chave_primaria: list[str]
) -> None:
    """
    Carrega um DataFrame para o SQL Server de forma performática.
    - Se a tabela de destino não existir, ela é criada.
    - Se a tabela existir, usa um MERGE para inserir/atualizar registros.
    """
    if df.empty:
        logger.warning(f"DataFrame para '{nome_tabela_final}' está vazio. Carga ignorada.")
        return

    # Validação de colunas da chave
    if not all(col in df.columns for col in chave_primaria):
        faltando = [col for col in chave_primaria if col not in df.columns]
        raise ValueError(f"Colunas da chave primária {faltando} não encontradas no DataFrame para a tabela '{nome_tabela_final}'.")

    # --- LÓGICA DE VERIFICAÇÃO E CRIAÇÃO DE TABELA ---
    insp = reflection.Inspector.from_engine(engine)
    if not insp.has_table(nome_tabela_final, schema='dbo'):
        logger.warning(f"A tabela de destino '{nome_tabela_final}' não existe. Criando-a e realizando carga inicial.")
        try:
            df.to_sql(nome_tabela_final, engine, if_exists='replace', index=False, schema='dbo')
            logger.info(f"Tabela '{nome_tabela_final}' criada e dados carregados com sucesso.")
            return # Finaliza a execução para este fluxo, pois a carga já foi feita
        except Exception as e:
            logger.exception(f"Falha ao tentar criar a tabela '{nome_tabela_final}'.")
            raise e

    # O fluxo de MERGE continua se a tabela já existir
    nome_tabela_temp = f"##{nome_tabela_final}_temp_upsert"
    logger.info(f"Iniciando carga de {len(df)} registros para a tabela temporária '{nome_tabela_temp}'...")

    try:
        df.to_sql(nome_tabela_temp, engine, if_exists='replace', index=False)
        logger.info("Carga para tabela temporária concluída.")

        colunas = [col for col in df.columns]
        colunas_str = ", ".join(f"[{col}]" for col in colunas)
        on_clause = " AND ".join(f"target.[{key}] = source.[{key}]" for key in chave_primaria)
        update_clause = ", ".join(f"target.[{col}] = source.[{col}]" for col in colunas if col not in chave_primaria)
        insert_values = ", ".join(f"source.[{col}]" for col in colunas)

        if not update_clause:
            update_clause = f"target.[{chave_primaria[0]}] = source.[{chave_primaria[0]}]"

        merge_sql = f"""
        MERGE [{nome_tabela_final}] AS target
        USING {nome_tabela_temp} AS source
        ON ({on_clause})
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({colunas_str})
            VALUES ({insert_values});
        """
        
        logger.info("Executando comando MERGE para sincronizar os dados...")
        with engine.begin() as connection:
            connection.execute(text(merge_sql))
        
        logger.info(f"Comando MERGE para '{nome_tabela_final}' executado com sucesso.")

    except Exception as e:
        logger.exception(f"ERRO AO EXECUTAR O PROCESSO DE MERGE PARA A TABELA '{nome_tabela_final}'")
        raise
    finally:
        try:
            with engine.begin() as connection:
                connection.execute(text(f"DROP TABLE IF EXISTS {nome_tabela_temp};"))
            logger.info(f"Tabela temporária '{nome_tabela_temp}' removida.")
        except Exception as drop_error:
            logger.warning(f"Não foi possível remover a tabela temporária '{nome_tabela_temp}'. Erro: {drop_error}")

