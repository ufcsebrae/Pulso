# processamento/enriquecimento.py (VERSÃO COM CORREÇÃO DO KEYERROR)
import logging
import pandas as pd
import argparse

# Importe as funções necessárias, pode ser necessário ajustar o caminho
from processamento.correcao_chaves import iniciar_correcao_interativa_chaves
from processamento.validacao import carregar_mapa_correcoes, aplicar_mapa_correcoes

logger = logging.getLogger(__name__)

CHAVES_MERGE = ["PROJETO", "ACAO", "UNIDADE", "ANO"]


def enriquecer_orcado_com_cc(
    df_orcado_pronto: pd.DataFrame, 
    df_cc_pronto: pd.DataFrame,
    args: argparse.Namespace
) -> pd.DataFrame:
    """
    Enriquece o DataFrame do Orçado com o CODCCUSTO e aciona o modo de
    correção interativa se houver falhas na junção.
    """
    logger.info("Iniciando a junção (merge) dos dados preparados...")

    df_cc_unico = df_cc_pronto.drop_duplicates(subset=CHAVES_MERGE, keep="first")
    
    # Adiciona um "indicador" para saber de onde veio a linha após o merge
    df_orcado_pronto['_merge_indicator'] = 'left_only'
    df_cc_unico['_merge_indicator'] = 'both'
    
    logger.info("Executando a junção com a chave: %s", CHAVES_MERGE)
    # Usamos um merge com um indicador para rastrear as falhas
    df_enriquecido = pd.merge(
        df_orcado_pronto,
        df_cc_unico.drop(columns=['CHAVE_CONCAT', 'CHAVE_CONCAT_original'], errors='ignore'), # Remove chaves de CC para não duplicar
        on=CHAVES_MERGE,
        how="left",
        suffixes=('', '_ref') # Adiciona sufixo para colunas de CC
    )

    # Identifica as linhas onde a junção falhou
    linhas_com_falha_mask = df_enriquecido['_merge_indicator_ref'].isnull()
    num_falhas = linhas_com_falha_mask.sum()

    if num_falhas > 0:
        logger.warning(
            "%d linhas do Orçado NÃO encontraram um CODCCUSTO correspondente.",
            num_falhas,
        )
        
        # --- CORREÇÃO DO KEYERROR ---
        # Acessamos a 'CHAVE_CONCAT' do DataFrame original (df_orcado_pronto)
        # usando a máscara de falha identificada no merge.
        chaves_com_falha = df_enriquecido.loc[linhas_com_falha_mask, "CHAVE_CONCAT"].unique()
        
        print("\n--- Chaves de Junção que Falharam ---")
        for chave in chaves_com_falha:
            print(f"  - {chave}")
        print("-" * 35)

        if args.modo_interativo:
            print("Ativando o modo de correção interativa para as chaves acima...")
            
            iniciar_correcao_interativa_chaves(set(chaves_com_falha), df_cc_unico)
            
            print("\nReaplicando correções após o modo interativo...")
            mapa_atualizado = carregar_mapa_correcoes()
            df_recorrigido = aplicar_mapa_correcoes(df_orcado_pronto.drop(columns=['_merge_indicator']), mapa_atualizado)
            
            # Tenta o merge novamente
            df_enriquecido = pd.merge(
                df_recorrigido,
                df_cc_unico.drop(columns=['CHAVE_CONCAT', 'CHAVE_CONCAT_original', '_merge_indicator'], errors='ignore'),
                on=CHAVES_MERGE,
                how="left",
            )
            novas_falhas = df_enriquecido["CODCCUSTO"].isnull().sum()
            logger.info(f"Após a correção, restam {novas_falhas} linhas sem correspondência.")
            
    else:
        logger.info("Sucesso! Todas as linhas do Orçado encontraram um CODCCUSTO.")

    # Limpa as colunas de indicador antes de retornar
    df_enriquecido.drop(columns=[col for col in df_enriquecido.columns if '_indicator' in col or '_ref' in col], inplace=True, errors='ignore')
    
    return df_enriquecido

