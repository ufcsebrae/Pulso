# processamento/preparacao.py
import logging
import pandas as pd

logger = logging.getLogger(__name__)

CHAVES_TEXTO = ["PROJETO", "ACAO", "UNIDADE"]
CHAVE_ANO = "ANO"

def preparar_dataframes_para_enriquecimento(
    df_orcado_raw: pd.DataFrame, df_cc_raw: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Orquestra a preparação completa dos DataFrames antes do enriquecimento.
    """
    # 1. Preparar DataFrame do Orçado
    logger.debug("Preparando DataFrame do Orçado...")
    df_orcado_renomeado = _renomear_colunas_orcado_fonte(df_orcado_raw)
    df_orcado_pronto = _padronizar_tipos_e_chaves(df_orcado_renomeado)

    # 2. Preparar DataFrame de CC
    logger.debug("Preparando DataFrame de CC...")
    df_cc_com_ano = _criar_coluna_ano_em_cc(df_cc_raw)
    df_cc_pronto = _padronizar_tipos_e_chaves(df_cc_com_ano)

    logger.debug("Colunas finais Orçado: %s", df_orcado_pronto.columns.tolist())
    logger.debug("Colunas finais CC: %s", df_cc_pronto.columns.tolist())

    return df_orcado_pronto, df_cc_pronto

def _padronizar_tipos_e_chaves(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza os tipos das colunas chave (texto e ano).
    Garante que as colunas essenciais existam antes de prosseguir.
    """
    df_copia = df.copy()
    colunas_a_verificar = CHAVES_TEXTO + [CHAVE_ANO]

    for col in colunas_a_verificar:
        if col not in df_copia.columns:
            raise ValueError(
                f"Preparação falhou: Coluna chave '{col}' não encontrada. "
                f"Colunas disponíveis: {df_copia.columns.tolist()}"
            )

    for col in CHAVES_TEXTO:
        df_copia[col] = df_copia[col].astype(str).str.strip().fillna("N/A")

    df_copia[CHAVE_ANO] = pd.to_numeric(df_copia[CHAVE_ANO], errors="coerce").fillna(0).astype(int)
    return df_copia

def _renomear_colunas_orcado_fonte(df: pd.DataFrame) -> pd.DataFrame:
    """Renomeia as colunas do DataFrame de orçamento bruto."""
    mapa_renomear = {
        '[Iniciativa].[Iniciativas].[Iniciativa].[MEMBER_CAPTION]': 'PROJETO',
        '[Ação].[Ação].[Nome de Ação].[MEMBER_CAPTION]': 'ACAO',
        '[Unidade Organizacional de Ação].[Unidade Organizacional de Ação].[Nome de Unidade Organizacional de Ação].[MEMBER_CAPTION]': 'UNIDADE',
        '[Tempo].[Ano].[Número Ano].[MEMBER_CAPTION]': 'ANO',
        '[Tempo].[Mês].[Número Mês].[MEMBER_CAPTION]': 'MES'
    }
    mapa_existente = {k: v for k, v in mapa_renomear.items() if k in df.columns}
    
    if not mapa_existente:
        logger.warning("Nenhuma coluna para renomear encontrada no DataFrame do Orçado.")
        return df

    return df.rename(columns=mapa_existente)

def _criar_coluna_ano_em_cc(df: pd.DataFrame) -> pd.DataFrame:
    """Cria a coluna 'ANO' no DataFrame de CC a partir de 'DTACAO'."""
    df_copia = df.copy()
    if 'ANO' in df_copia.columns:
        return df_copia

    date_col = 'DTACAO'
    if date_col not in df_copia.columns:
        raise ValueError(f"Preparação falhou: Coluna '{date_col}' não encontrada no DataFrame de CC.")

    df_copia['ANO'] = pd.to_datetime(df_copia[date_col], errors='coerce').dt.year
    return df_copia
