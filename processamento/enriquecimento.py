# processamento/enriquecimento.py
import logging
import pandas as pd

logger = logging.getLogger(__name__)

CHAVES_MERGE = ["PROJETO", "ACAO", "UNIDADE", "ANO"]

def enriquecer_orcado_com_cc(
    df_orcado_pronto: pd.DataFrame, df_cc_pronto: pd.DataFrame
) -> pd.DataFrame:
    """
    Enriquece o DataFrame do Orçado com o CODCCUSTO da estrutura de referência.
    Assume que ambos os DataFrames já foram limpos e preparados.
    """
    logger.info("Iniciando a junção (merge) dos dados preparados...")

    df_cc_unico = df_cc_pronto.drop_duplicates(subset=CHAVES_MERGE, keep="first")
    logger.debug(
        "Estrutura de CC reduzida de %d para %d linhas após remoção de duplicatas.",
        len(df_cc_pronto),
        len(df_cc_unico),
    )

    logger.info("Executando a junção com a chave: %s", CHAVES_MERGE)
    df_enriquecido = pd.merge(
        df_orcado_pronto,
        df_cc_unico,
        on=CHAVES_MERGE,
        how="left",
    )

    linhas_sem_cc = df_enriquecido["CODCCUSTO"].isnull().sum()
    if linhas_sem_cc > 0:
        logger.warning(
            "%d linhas do Orçado NÃO encontraram um CODCCUSTO correspondente.",
            linhas_sem_cc,
        )
    else:
        logger.info("Sucesso! Todas as linhas do Orçado encontraram um CODCCUSTO.")

    return df_enriquecido
