# processamento/validacao.py
import json
import logging
from pathlib import Path
from typing import Dict

import pandas as pd
from config import CONFIG

logger = logging.getLogger(__name__)

def preparar_dados_para_validacao(
    df_raw: pd.DataFrame, chaves_base: list[str], incluir_ano_na_chave: bool = False
) -> pd.DataFrame:
    """
    Prepara um DataFrame para validação, criando uma chave concatenada.
    """
    df = df_raw.copy()
    
    if '[Measures].[ValorAjustado]' in df.columns:
        logger.debug("DataFrame do Orçado detectado. Renomeando e preparando...")
        df = _renomear_colunas_orcado_fonte(df)
        
        # *** PASSO 1: SALVANDO O ANO ORIGINAL ***
        # Cria uma cópia da coluna 'ANO' antes de qualquer possível alteração.
        if 'ANO' in df.columns:
            logger.debug("Preservando o ano original na coluna 'ANO_FOTOGRAFIA'.")
            df['ANO_FOTOGRAFIA'] = df['ANO']

    elif 'CODCCUSTO' in df.columns:
        logger.debug("DataFrame de CC detectado. Criando coluna 'ANO'...")
        df = _criar_coluna_ano_em_cc(df)
        
    colunas_chave = chaves_base + ['ANO'] if incluir_ano_na_chave else chaves_base
    
    for col in colunas_chave:
        if col not in df.columns:
            raise KeyError(f"Coluna essencial '{col}' não encontrada após a preparação.")
        if col != 'ANO':
            df[col] = df[col].astype(str).str.strip().fillna('N/A')

    df['ANO'] = pd.to_numeric(df['ANO'], errors='coerce').fillna(0).astype(int)
    
    df['CHAVE_CONCAT'] = (
        df[chaves_base].agg('|'.join, axis=1) + '|' + df['ANO'].astype(str)
    )
    
    return df

# ... (o resto do arquivo permanece exatamente como na última versão)
def aplicar_mapa_correcoes(df: pd.DataFrame, mapa_correcoes: Dict[str, str]) -> pd.DataFrame:
    if not mapa_correcoes:
        df['CHAVE_CONCAT_original'] = df['CHAVE_CONCAT']
        return df
    df_copy = df.copy()
    df_copy['CHAVE_CONCAT_original'] = df_copy['CHAVE_CONCAT']
    linhas_para_corrigir_idx = df_copy.index[df_copy['CHAVE_CONCAT'].isin(mapa_correcoes.keys())]
    if linhas_para_corrigir_idx.empty:
        return df_copy
    logger.info("Aplicando %d correções conhecidas em %d linhas...", len(mapa_correcoes), len(linhas_para_corrigir_idx))
    def aplicar_correcao_linha(row):
        chave_original = row['CHAVE_CONCAT']
        chave_corrigida_str = mapa_correcoes.get(chave_original)
        if chave_corrigida_str:
            try:
                partes = chave_corrigida_str.split('|')
                row['PROJETO'], row['ACAO'], row['UNIDADE'], row['ANO'] = partes[0], partes[1], partes[2], int(partes[3])
            except (IndexError, ValueError) as e:
                logger.error("Erro ao desmontar chave corrigida '%s': %s", chave_corrigida_str, e)
        return row
    df_copy.loc[linhas_para_corrigir_idx] = df_copy.loc[linhas_para_corrigir_idx].apply(aplicar_correcao_linha, axis=1)
    df_copy['ANO'] = df_copy['ANO'].astype(int)
    return df_copy

def carregar_mapa_correcoes() -> Dict[str, str]:
    caminho_mapa = CONFIG.paths.mapa_correcoes
    if not caminho_mapa.exists():
        return {}
    with open(caminho_mapa, 'r', encoding='utf-8') as f:
        return json.load(f)

def salvar_mapa_correcoes(mapa: Dict[str, str]):
    caminho_mapa = CONFIG.paths.mapa_correcoes
    with open(caminho_mapa, 'w', encoding='utf-8') as f:
        json.dump(mapa, f, indent=4, ensure_ascii=False, sort_keys=True)
    logger.info("Mapa de correções salvo com sucesso em '%s'.", caminho_mapa.name)

def _renomear_colunas_orcado_fonte(df: pd.DataFrame) -> pd.DataFrame:
    mapa_renomear_completo = {
        '[Iniciativa].[Iniciativas].[Iniciativa].[MEMBER_CAPTION]': 'PROJETO',
        '[Ação].[Ação].[Nome de Ação].[MEMBER_CAPTION]': 'ACAO',
        '[Unidade Organizacional de Ação].[Unidade Organizacional de Ação].[Nome de Unidade Organizacional de Ação].[MEMBER_CAPTION]': 'UNIDADE',
        '[Tempo].[Ano].[Número Ano].[MEMBER_CAPTION]': 'ANO',
        '[Tempo].[Mês].[Número Mês].[MEMBER_CAPTION]': 'MES',
        '[PPA].[PPA com Fotografia].[Descrição de PPA com Fotografia].[MEMBER_CAPTION]': 'Descricao_PPA',
        '[Natureza Orçamentária].[Código Estruturado 4 nível].[Código Estruturado 4 nível].[MEMBER_CAPTION]': 'Codigo_Natureza_Orcamentaria',
        '[Natureza Orçamentária].[Descrição de Natureza 4 nível].[Descrição de Natureza 4 nível].[MEMBER_CAPTION]': 'Descricao_Natureza_Orcamentaria',
        '[Measures].[ValorAjustado]': 'Valor_Ajustado'
    }
    df_renomeado = df.rename(columns=mapa_renomear_completo)
    if 'UNIDADE' in df_renomeado.columns:
        logger.info("Padronizando coluna 'UNIDADE' (removendo prefixo 'SP - ')...")
        df_renomeado['UNIDADE'] = df_renomeado['UNIDADE'].str.replace('SP - ', '', regex=False)
    return df_renomeado

def _criar_coluna_ano_em_cc(df: pd.DataFrame) -> pd.DataFrame:
    df_copia = df.copy()
    if 'ANO' in df_copia.columns:
        return df_copia
    date_col = 'DTACAO'
    if date_col not in df_copia.columns:
        raise KeyError(f"Coluna de data '{date_col}' não encontrada para criar o 'ANO' no DataFrame de CC.")
    df_copia['ANO'] = pd.to_datetime(df_copia[date_col], errors='coerce').dt.year
    return df_copia
