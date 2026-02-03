# processamento/correcao_chaves.py
import logging
import pandas as pd
from sqlalchemy import text

logger = logging.getLogger(__name__)

def salvar_correcao_no_sql(chave_quebrada: str, chave_correta: str):
    """
    Salva (ou atualiza) uma única correção na tabela MapaCorrecoesChaves do SQL Server.
    """
    from config.database import get_conexao
    from config.config import CONFIG
    
    engine = get_conexao(CONFIG.conexoes["FINANCA_SQL"])

    stmt = text("""
        MERGE dbo.MapaCorrecoesChaves AS target
        USING (SELECT :quebrada AS ChaveQuebrada, :correta AS ChaveCorreta) AS source
        ON (target.ChaveQuebrada = source.ChaveQuebrada)
        WHEN MATCHED THEN
            UPDATE SET ChaveCorreta = source.ChaveCorreta, DataCriacao = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (ChaveQuebrada, ChaveCorreta) VALUES (source.ChaveQuebrada, source.ChaveCorreta);
    """)
    
    try:
        with engine.begin() as connection:
            connection.execute(stmt, {"quebrada": chave_quebrada, "correta": chave_correta})
        logger.info(f"Correção para '{chave_quebrada}' salva no SQL Server.")
    except Exception:
        logger.exception("Falha ao salvar correção no SQL Server.")

def iniciar_correcao_interativa_chaves(chaves_com_falha: set, df_referencia: pd.DataFrame):
    """
    Inicia o modo interativo para corrigir chaves não encontradas, salvando no SQL.
    """
    logger.info("--- MODO DE CORREÇÃO INTERATIVA ---")
    chaves_referencia_validas = set(df_referencia['CHAVE_CONCAT'])
    
    for i, chave_errada in enumerate(chaves_com_falha):
        print(f"\n[{i+1}/{len(chaves_com_falha)}] Chave com erro: {chave_errada}")
        
        while True:
            nova_chave = input("Digite a chave correta (ou 'p' para pular, 's' para sair): ").strip()
            
            if nova_chave.lower() == 's':
                logger.info("Saindo do modo interativo.")
                return
            if nova_chave.lower() == 'p':
                logger.warning(f"Chave '{chave_errada}' pulada.")
                break
                
            if nova_chave in chaves_referencia_validas:
                salvar_correcao_no_sql(chave_errada, nova_chave)
                print(">>> Correção válida e salva no banco de dados!")
                break
            else:
                print("!!! Erro: A chave digitada não existe na tabela de referência. Tente novamente.")
