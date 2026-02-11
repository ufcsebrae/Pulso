# processamento/correcao_chaves.py (VERSÃO FINAL COM TRATAMENTO DE 'PULAR')
import logging
import pandas as pd
from sqlalchemy import text
from fuzzywuzzy import process

logger = logging.getLogger(__name__)

def salvar_correcao_no_sql(chave_quebrada: str, chave_correta: str):
    from config.database import get_conexao
    from config.config import CONFIG
    engine = get_conexao(CONFIG.conexoes["FINANCA_SQL"])
    stmt = text("""
        MERGE dbo.MapaCorrecoesChaves AS target
        USING (SELECT :quebrada AS ChaveQuebrada, :correta AS ChaveCorreta) AS source
        ON (target.ChaveQuebrada = source.ChaveQuebrada)
        WHEN MATCHED THEN UPDATE SET ChaveCorreta = source.ChaveCorreta
        WHEN NOT MATCHED THEN INSERT (ChaveQuebrada, ChaveCorreta) VALUES (source.ChaveQuebrada, source.ChaveCorreta);
    """)
    try:
        with engine.begin() as connection:
            connection.execute(stmt, {"quebrada": chave_quebrada, "correta": chave_correta})
        logger.info(f"Correção para '{chave_quebrada}' salva no SQL Server com sucesso.")
    except Exception as e:
        logger.exception("Falha ao salvar correção no SQL Server.")
        raise e

def _obter_sugestao_interativa(parte_chave_errada: str, opcoes_validas: list, nome_campo: str) -> str:
    if not opcoes_validas:
        print(f"\n!!! Alerta: Nenhuma opção válida encontrada para '{nome_campo}' com os filtros atuais.")
        return parte_chave_errada # Retorna o original para indicar que não houve mudança

    print(f"\n--- Corrigindo '{nome_campo}': '{parte_chave_errada}' ---")
    sugestoes = process.extract(parte_chave_errada, opcoes_validas, limit=5)
    print("Sugestões encontradas:")
    for i, (sugestao, pontuacao) in enumerate(sugestoes):
        print(f"  {i+1}) {sugestao} (similaridade: {pontuacao}%)")
    
    while True:
        entrada = input(f"Escolha o número, digite o valor manual ou 'p' para pular: ").strip()
        if entrada.lower() == 'p':
            print(f">>> {nome_campo} pulado. Mantendo valor original: '{parte_chave_errada}'")
            return parte_chave_errada
        try:
            indice = int(entrada) - 1
            if 0 <= indice < len(sugestoes):
                valor_correto = sugestoes[indice][0]
                print(f">>> Selecionado: '{valor_correto}'")
                return valor_correto
            else:
                print("!!! Número de sugestão inválido.")
        except ValueError:
            if entrada in opcoes_validas:
                print(f">>> Valor manual válido: '{entrada}'")
                return entrada
            else:
                print("!!! Valor manual não encontrado nas opções. Tente novamente.")

def iniciar_correcao_interativa_chaves(chaves_com_falha: set, df_referencia: pd.DataFrame):
    logger.info("--- MODO DE CORREÇÃO INTERATIVA (COM SUGESTÕES CONTEXTUAIS) ---")
    
    for i, chave_errada in enumerate(chaves_com_falha):
        print(f"\n=======================================================")
        print(f"[{i+1}/{len(chaves_com_falha)}] Corrigindo Chave Quebrada: {chave_errada}")
        print(f"=======================================================")
        
        try:
            proj_errado, acao_errada, unidade_errada, ano_errado = chave_errada.split('|')
        except ValueError:
            logger.warning(f"Chave '{chave_errada}' está em um formato inválido e será pulada.")
            continue

        # --- LÓGICA DE CORREÇÃO COM TRATAMENTO PARA 'PULAR' ---

        # 1. Corrigir PROJETO
        projetos_validos = df_referencia['PROJETO'].unique().tolist()
        proj_correto = _obter_sugestao_interativa(proj_errado, projetos_validos, "PROJETO")
        
        # Se o usuário pulou e o projeto não existe, não há como continuar
        if proj_correto not in projetos_validos:
            logger.warning(f"Projeto '{proj_correto}' não é válido ou foi pulado. Não é possível continuar a correção para esta chave.")
            continue # Pula para a próxima chave com falha

        # 2. Corrigir AÇÃO
        df_filtrado_por_projeto = df_referencia[df_referencia['PROJETO'] == proj_correto]
        acoes_validas = df_filtrado_por_projeto['ACAO'].unique().tolist()
        acao_correta = _obter_sugestao_interativa(acao_errada, acoes_validas, "AÇÃO")
        
        if acao_correta not in acoes_validas:
            logger.warning(f"Ação '{acao_correta}' não é válida ou foi pulada. Não é possível continuar a correção para esta chave.")
            continue

        # 3. Corrigir UNIDADE
        df_filtrado_por_acao = df_filtrado_por_projeto[df_filtrado_por_projeto['ACAO'] == acao_correta]
        unidades_validas = df_filtrado_por_acao['UNIDADE'].unique().tolist()
        unidade_correta = _obter_sugestao_interativa(unidade_errada, unidades_validas, "UNIDADE")

        if unidade_correta not in unidades_validas:
            logger.warning(f"Unidade '{unidade_correta}' não é válida ou foi pulada. A correção para esta chave será descartada.")
            continue
            
        chave_correta_final = f"{proj_correto}|{acao_correta}|{unidade_correta}|{ano_errado}"
        
        print(f"\nChave Original : {chave_errada}")
        print(f"Chave Corrigida: {chave_correta_final}")
        
        # Não salva se a chave não mudou
        if chave_correta_final == chave_errada:
            print("Nenhuma alteração feita. Correção não será salva.")
            continue
            
        confirmacao = input("Salvar esta correção no banco de dados? (s/n): ").strip().lower()
        if confirmacao == 's':
            try:
                salvar_correcao_no_sql(chave_errada, chave_correta_final)
            except Exception:
                logger.error("A execução foi interrompida devido a uma falha ao salvar no banco de dados.")
                return # Interrompe a execução para evitar mais erros
        else:
            logger.warning(f"Correção para '{chave_errada}' foi descartada pelo usuário.")
