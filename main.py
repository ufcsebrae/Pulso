# main.py
import argparse
import logging
import sys
import pandas as pd

# 1. INICIALIZAÇÃO (sem alterações)
try:
    from config.logger_config import configurar_logger
    configurar_logger("pipeline_principal.log")
    from config.inicializacao import carregar_drivers_externos
    carregar_drivers_externos()
except Exception as e:
    logging.basicConfig(level=logging.INFO)
    logging.critical("Falha na inicialização: %s", e, exc_info=True)
    sys.exit(1)

# 2. IMPORTAÇÕES
from config.config import CONFIG
from config.database import get_conexao
from comunicacao.carregamento import carregar_dataframe_para_sql_sem_duplicados
# --- ALTERADO: Importando a nova função de extração ---
from processamento.extracao import obter_dados_brutos, obter_dados_comprometidos_brutos
from processamento.correcao_chaves import iniciar_correcao_interativa_chaves
from processamento.enriquecimento import enriquecer_orcado_com_cc
from processamento.validacao import (
    aplicar_mapa_correcoes,
    carregar_mapa_correcoes,
    preparar_dados_para_validacao,
)

logger = logging.getLogger(__name__)

# --- NOVA FUNÇÃO GENÉRICA DE ENRIQUECIMENTO ---
def executar_fluxo_de_enriquecimento(
    df_raw: pd.DataFrame,
    df_cc_referencia: pd.DataFrame,
    mapa_correcoes: dict,
    nome_fluxo: str,
    args: argparse.Namespace
) -> pd.DataFrame:
    """
    Motor genérico que executa o processo de enriquecimento para um DataFrame de entrada.
    """
    logger.info(f"--- Iniciando fluxo de enriquecimento para: {nome_fluxo} ---")
    if df_raw.empty:
        logger.warning(f"DataFrame de entrada para '{nome_fluxo}' está vazio. Fluxo ignorado.")
        return pd.DataFrame()

    # 1. PREPARAÇÃO
    chaves_base = ['PROJETO', 'ACAO', 'UNIDADE']
    df_preparado = preparar_dados_para_validacao(df_raw, chaves_base, incluir_ano_na_chave=True)

    # 2. APLICAÇÃO DE CORREÇÕES
    df_corrigido = aplicar_mapa_correcoes(df_preparado, mapa_correcoes)

    # 3. ENRIQUECIMENTO
    df_enriquecido = enriquecer_orcado_com_cc(df_corrigido, df_cc_referencia)

    # 4. TRATAMENTO DE FALHAS (se houver)
    # A função original foi movida para cá
    df_falhas = df_enriquecido[df_enriquecido['CODCCUSTO'].isnull()]
    if not df_falhas.empty:
        chaves_com_falha = set(df_falhas['CHAVE_CONCAT_original'])
        logger.warning(f"--- ATENÇÃO [{nome_fluxo}]: {len(chaves_com_falha)} combinações únicas não foram enriquecidas ---")
        if args.modo_interativo:
            iniciar_correcao_interativa_chaves(chaves_com_falha, df_cc_referencia)
            # Após a correção, o mapa precisa ser recarregado na próxima chamada
            logger.info("Processo de correção finalizado. O mapa no SQL foi atualizado.")
        else:
            print(f"\nPara corrigir as falhas de '{nome_fluxo}', execute com a flag: python main.py --modo-interativo")
    
    return df_enriquecido

# --- FUNÇÃO ORQUESTRADORA PRINCIPAL ---
def run_pipelines_principais(args: argparse.Namespace) -> None:
    """
    Orquestra a execução das pipelines de enriquecimento para todas as fontes de dados.
    """
    # 1. Obter dados que são COMUNS a todas as pipelines
    _, df_cc_raw = obter_dados_brutos()
    mapa_correcoes = carregar_mapa_correcoes()
    chaves_base = ['PROJETO', 'ACAO', 'UNIDADE']
    df_cc_referencia = preparar_dados_para_validacao(df_cc_raw, chaves_base, incluir_ano_na_chave=True)
    engine_financa = get_conexao(CONFIG.conexoes["FINANCA_SQL"])

    # 2. EXECUTAR PIPELINE PARA O ORÇADO
    df_orcado_raw, _ = obter_dados_brutos()
    df_orcado_final = executar_fluxo_de_enriquecimento(
        df_raw=df_orcado_raw,
        df_cc_referencia=df_cc_referencia,
        mapa_correcoes=mapa_correcoes,
        nome_fluxo="Orçado Nacional",
        args=args
    )
    if not df_orcado_final.empty:
        # Salvar o resultado
        logger.info("Salvando resultado do 'Orçado Nacional'...")
        salvar_resultado_no_sql(df_orcado_final, "ORCADO_ENRIQUECIDO_COM_CC", engine_financa)

    # --- NOVA ETAPA: EXECUTAR PIPELINE PARA O COMPROMETIDO ---
    # Recarrega o mapa caso ele tenha sido alterado no modo interativo
    if args.modo_interativo:
        mapa_correcoes = carregar_mapa_correcoes()

    df_comprometido_raw = obter_dados_comprometidos_brutos()
    df_comprometido_final = executar_fluxo_de_enriquecimento(
        df_raw=df_comprometido_raw,
        df_cc_referencia=df_cc_referencia,
        mapa_correcoes=mapa_correcoes,
        nome_fluxo="Comprometido Nacional",
        args=args
    )
    if not df_comprometido_final.empty:
        logger.info("Salvando resultado do 'Comprometido Nacional'...")
        # ATENÇÃO: Defina o nome da sua nova tabela de destino!
        salvar_resultado_no_sql(df_comprometido_final, "COMPROMETIDO_ENRIQUECIDO_COM_CC", engine_financa)

def salvar_resultado_no_sql(df_para_salvar: pd.DataFrame, nome_tabela: str, engine):
    """Função auxiliar para organizar colunas e salvar no SQL."""
    logger.info(f"Organizando colunas para a tabela final '{nome_tabela}'...")
    if 'ANO_FOTOGRAFIA' in df_para_salvar.columns:
        df_para_salvar['ANO'] = df_para_salvar['ANO_FOTOGRAFIA']

    colunas_finais = [
        'ANO', 'MES', 'PROJETO', 'ACAO', 'UNIDADE', 'CODCCUSTO', 'Valor_Ajustado',
        'Descricao_PPA', 'Codigo_Natureza_Orcamentaria', 'Descricao_Natureza_Orcamentaria',
        'DTUNIDADE', 'DTPROJETO', 'DTACAO'
    ]
    colunas_presentes = [col for col in colunas_finais if col in df_para_salvar.columns]
    df_final = df_para_salvar[colunas_presentes].copy()
    df_final.dropna(subset=['CODCCUSTO'], inplace=True)

    carregar_dataframe_para_sql_sem_duplicados(
        df=df_final,
        nome_tabela=nome_tabela,
        engine=engine,
        chave_primaria=['CODCCUSTO'] # Assumindo a mesma chave. Adapte se for diferente.
    )
    logger.info(f"Processo para a tabela '{nome_tabela}' concluído com sucesso.")

def main() -> None:
    """Ponto de entrada principal da aplicação."""
    parser = argparse.ArgumentParser(description="Robô de Enriquecimento de Dados.")
    parser.add_argument("--modo-interativo", action="store_true", help="Ativa o modo interativo para correção de chaves.")
    args = parser.parse_args()
    
    logger.info("--- INICIANDO ROBÔ DE ENRIQUECIMENTO DE DADOS ---")
    if args.modo_interativo:
        logger.info("Modo interativo ATIVADO.")
    try:
        run_pipelines_principais(args)
    except Exception:
        logger.exception("--- ERRO CRÍTICO E INESPERADO NA EXECUÇÃO ---")
    finally:
        logger.info("--- FIM DA EXECUÇÃO ---")

if __name__ == "__main__":
    main()
