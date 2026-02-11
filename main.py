# main.py (VERSÃO FINAL COM LÓGICA DE JUNÇÃO CORRIGIDA)
import argparse
import logging
import sys
import pandas as pd

try:
    from config.logger_config import configurar_logger
    configurar_logger("pipeline_principal.log")
    from config.inicializacao import carregar_drivers_externos
    carregar_drivers_externos()
except Exception as e:
    logging.basicConfig(level=logging.INFO); logging.critical("Falha na inicialização: %s", e, exc_info=True); sys.exit(1)

from config.config import CONFIG
from config.database import get_conexao
from comunicacao.carregamento import carregar_dataframe_para_sql_com_merge
from processamento.extracao import obter_dados_brutos, obter_dados_comprometidos_brutos
from processamento.correcao_chaves import iniciar_correcao_interativa_chaves
from processamento.validacao import aplicar_mapa_correcoes, carregar_mapa_correcoes, preparar_dados_para_validacao

logger = logging.getLogger(__name__)

def executar_fluxo_de_enriquecimento(df_raw: pd.DataFrame, df_cc_referencia: pd.DataFrame, mapa_correcoes: dict, nome_fluxo: str, args: argparse.Namespace) -> pd.DataFrame:
    logger.info(f"--- Iniciando fluxo de enriquecimento para: {nome_fluxo} ---")
    if df_raw.empty:
        return pd.DataFrame()

    df_para_validar = df_raw.copy()

    if nome_fluxo == "Comprometido Nacional":
        logger.info("Fluxo 'Comprometido' detectado. Enriquecendo com dados de CC ANTES da validação de chaves.")
        df_cc_ref_comprometido = df_cc_referencia.copy()
        
        logger.info("Aplicando truncagem de chave na referência de CC para correspondência.")
        df_cc_ref_comprometido['CODCCUSTO_TRUNCADO'] = df_cc_ref_comprometido['CODCCUSTO'].astype(str).str.rsplit('.', n=1).str[0]
        df_para_validar['CODCCUSTO'] = df_para_validar['CODCCUSTO'].astype(str).str.strip()
        
        # Faz a junção para trazer as colunas PROJETO, ACAO, UNIDADE e o CODCCUSTO completo
        df_enriquecido_inicial = pd.merge(
            df_para_validar,
            df_cc_ref_comprometido[['CODCCUSTO_TRUNCADO', 'PROJETO', 'ACAO', 'UNIDADE', 'CODCCUSTO']].rename(columns={'CODCCUSTO': 'CODCCUSTO_COMPLETO'}),
            left_on='CODCCUSTO',
            right_on='CODCCUSTO_TRUNCADO',
            how='left'
        )
        # O DataFrame agora tem 'CODCCUSTO' (o antigo, truncado) e 'CODCCUSTO_COMPLETO'
        # Remove o antigo e renomeia o completo para ser a chave principal
        df_enriquecido_inicial.drop(columns=['CODCCUSTO', 'CODCCUSTO_TRUNCADO'], inplace=True)
        df_enriquecido_inicial.rename(columns={'CODCCUSTO_COMPLETO': 'CODCCUSTO'}, inplace=True)
        
        linhas_sem_match = df_enriquecido_inicial['PROJETO'].isnull().sum()
        if linhas_sem_match > 0:
            logger.warning(f"{linhas_sem_match} linhas do Comprometido não encontraram correspondência e serão descartadas.")
            df_enriquecido_inicial.dropna(subset=['PROJETO'], inplace=True)
        if df_enriquecido_inicial.empty:
            logger.error("Nenhum dado do Comprometido restou após a junção. Verifique o formato dos 'CODCCUSTO'."); return pd.DataFrame()
        
        # O DataFrame agora está pronto para validação, já com as colunas corretas.
        df_para_validar = df_enriquecido_inicial

    chaves_base = ['PROJETO', 'ACAO', 'UNIDADE']
    df_preparado = preparar_dados_para_validacao(df_para_validar, chaves_base, incluir_ano_na_chave=True)
    df_corrigido = aplicar_mapa_correcoes(df_preparado, mapa_correcoes)
    
    # Validação final para garantir que CODCCUSTO ainda existe antes de salvar
    if 'CODCCUSTO' not in df_corrigido.columns:
        logger.error("Coluna 'CODCCUSTO' foi perdida durante o processo de validação/correção.")
        return pd.DataFrame()

    return df_corrigido


# O restante do arquivo permanece o mesmo
def run_pipelines_principais(args: argparse.Namespace) -> None:
    _, df_cc_raw = obter_dados_brutos()
    mapa_correcoes = carregar_mapa_correcoes()
    chaves_base = ['PROJETO', 'ACAO', 'UNIDADE']
    df_cc_referencia = preparar_dados_para_validacao(df_cc_raw, chaves_base, incluir_ano_na_chave=True)
    engine_financa = get_conexao(CONFIG.conexoes["FINANCA_SQL"])
    df_orcado_raw, _ = obter_dados_brutos()
    df_orcado_final = executar_fluxo_de_enriquecimento(df_raw=df_orcado_raw, df_cc_referencia=df_cc_referencia, mapa_correcoes=mapa_correcoes, nome_fluxo="Orçado Nacional", args=args)
    if not df_orcado_final.empty:
        logger.info("Salvando resultado do 'Orçado Nacional'...")
        salvar_resultado_no_sql(df_orcado_final, "ORCADO_ENRIQUECIDO_COM_CC", engine_financa)
    if args.modo_interativo: mapa_correcoes = carregar_mapa_correcoes()
    df_comprometido_raw = obter_dados_comprometidos_brutos()
    df_comprometido_final = executar_fluxo_de_enriquecimento(df_raw=df_comprometido_raw, df_cc_referencia=df_cc_referencia, mapa_correcoes=mapa_correcoes, nome_fluxo="Comprometido Nacional", args=args)
    if not df_comprometido_final.empty:
        logger.info("Salvando resultado do 'Comprometido Nacional'...")
        salvar_resultado_no_sql(df_comprometido_final, "COMPROMETIDO_ENRIQUECIDO_COM_CC", engine_financa)


def salvar_resultado_no_sql(df_para_salvar: pd.DataFrame, nome_tabela: str, engine):
    logger.info(f"Organizando colunas para a tabela final '{nome_tabela}'...")
    if 'COMPROMETIDO' in df_para_salvar.columns: df_para_salvar.rename(columns={'COMPROMETIDO': 'Valor_Ajustado'}, inplace=True)
    chave_primaria = ['ANO', 'MES', 'CODCCUSTO', 'PROJETO', 'ACAO', 'Codigo_Natureza_Orcamentaria']
    chave_existente = [col for col in chave_primaria if col in df_para_salvar.columns]
    logger.info(f"Garantindo unicidade dos registros com base na chave: {chave_existente}")
    regras_agg = {col: ('sum' if pd.api.types.is_numeric_dtype(df_para_salvar[col]) else 'first') for col in df_para_salvar.columns if col not in chave_existente}
    df_agregado = df_para_salvar.groupby(chave_existente, as_index=False).agg(regras_agg)
    if len(df_agregado) < len(df_para_salvar): logger.warning(f"Foram agregadas {len(df_para_salvar) - len(df_agregado)} linhas duplicadas.")
    colunas_finais = ['ANO', 'MES', 'PROJETO', 'ACAO', 'UNIDADE', 'CODCCUSTO', 'Valor_Ajustado', 'Codigo_Natureza_Orcamentaria']
    colunas_presentes = [col for col in colunas_finais if col in df_agregado.columns]
    df_final = df_agregado[colunas_presentes].copy()
    df_final.dropna(subset=['CODCCUSTO'], inplace=True)
    carregar_dataframe_para_sql_com_merge(df=df_final, nome_tabela_final=nome_tabela, engine=engine, chave_primaria=chave_existente)
    logger.info(f"Processo para a tabela '{nome_tabela}' concluído com sucesso.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Robô de Enriquecimento de Dados.")
    parser.add_argument("--modo-interativo", action="store_true", help="Ativa o modo interativo para correção de chaves.")
    args = parser.parse_args()
    logger.info("--- INICIANDO ROBÔ DE ENRIQUECIMENTO DE DADOS ---")
    if args.modo_interativo: logger.info("Modo interativo ATIVADO.")
    try:
        run_pipelines_principais(args)
    except Exception:
        logger.exception("--- ERRO CRÍTICO E INESPERADO NA EXECUÇÃO ---")
    finally:
        logger.info("--- FIM DA EXECUÇÃO ---")

if __name__ == "__main__":
    main()
