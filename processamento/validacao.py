# processamento/validacao.py
import json
import logging
import pandas as pd
from sqlalchemy.exc import ProgrammingError
from typing import Dict

from config.config import CONFIG

logger = logging.getLogger(__name__)

# --- FUNÇÕES MODIFICADAS ---

def carregar_mapa_correcoes() -> Dict[str, str]:
    from config.database import get_conexao
    logger.info("Carregando mapa de correções do SQL Server...")
    engine = get_conexao(CONFIG.conexoes["FINANCA_SQL"])
    try:
        query = "SELECT ChaveQuebrada, ChaveCorreta FROM dbo.MapaCorrecoesChaves"
        df_mapa = pd.read_sql(query, engine)
        if df_mapa.empty:
            logger.info("Nenhuma correção encontrada na tabela 'MapaCorrecoesChaves'.")
            return {}
        mapa = pd.Series(df_mapa.ChaveCorreta.values, index=df_mapa.ChaveQuebrada).to_dict()
        logger.info(f"{len(mapa)} correções carregadas do banco de dados.")
        return mapa
    except (ProgrammingError, Exception) as e:
        logger.warning("Tabela 'MapaCorrecoesChaves' não encontrada. Continuando com mapa vazio. Erro: %s", e)
        return {}

def preparar_dados_para_validacao(
    df_raw: pd.DataFrame, chaves_base: list[str], incluir_ano_na_chave: bool = False
) -> pd.DataFrame:
    """
    Prepara um DataFrame para validação, criando uma chave concatenada.
    Agora é resiliente a dados brutos ou dados já processados (do cache).
    """
    df = df_raw.copy()

    # --- ALTERAÇÃO PRINCIPAL PARA CORRIGIR O KEYERROR ---
    # Verifica se os dados são brutos (precisam ser renomeados) e se ainda não foram renomeados.
    if '[Measures].[ValorAjustado]' in df.columns and 'PROJETO' not in df.columns:
        logger.info("Dados brutos detectados. Renomeando colunas do formato de origem...")
        df = _renomear_colunas_orcado_fonte(df)
        if 'ANO' in df.columns:
            df['ANO_FOTOGRAFIA'] = df['ANO']
    elif 'CODCCUSTO' in df.columns:
        logger.info("DataFrame de CC detectado. Garantindo a existência da coluna 'ANO'...")
        df = _criar_coluna_ano_em_cc(df)
    
    # O resto da função continua, agora com a garantia de que as colunas corretas existem.
    colunas_chave = chaves_base + ['ANO'] if incluir_ano_na_chave else chaves_base
    
    for col in colunas_chave:
        if col not in df.columns:
            # Este erro agora indicaria um problema mais sério
            raise KeyError(f"Coluna essencial '{col}' não encontrada após a preparação inicial.")
        if col != 'ANO':
            df[col] = df[col].astype(str).str.strip().fillna('N/A')

    df['ANO'] = pd.to_numeric(df['ANO'], errors='coerce').fillna(0).astype(int)
    
    df['CHAVE_CONCAT'] = (
        df[chaves_base].agg('|'.join, axis=1) + '|' + df['ANO'].astype(str)
    )
    
    return df

# --- FUNÇÕES ORIGINAIS (SEM ALTERAÇÃO) ---

def aplicar_mapa_correcoes(df: pd.DataFrame, mapa_correcoes: Dict[str, str]) -> pd.DataFrame:
    if not mapa_correcoes:
        df['CHAVE_CONCAT_original'] = df['CHAVE_CONCAT']
        return df
    
    df_copy = df.copy()
    df_copy['CHAVE_CONCAT_original'] = df_copy['CHAVE_CONCAT']
    
    linhas_para_corrigir_idx = df_copy.index[df_copy['CHAVE_CONCAT'].isin(mapa_correcoes.keys())]
    if not linhas_para_corrigir_idx.empty:
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

def _renomear_colunas_orcado_fonte(df: pd.DataFrame) -> pd.DataFrame:
    mapa_renomear = {
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
    df_renomeado = df.rename(columns=mapa_renomear)
    if 'UNIDADE' in df_renomeado.columns:
        df_renomeado['UNIDADE'] = df_renomeado['UNIDADE'].str.replace('SP - ', '', regex=False)
    return df_renomeado

def _criar_coluna_ano_em_cc(df: pd.DataFrame) -> pd.DataFrame:
    df_copia = df.copy()
    if 'ANO' in df_copia.columns: return df_copia
    date_col = 'DTACAO'
    if date_col not in df_copia.columns: raise KeyError(f"'{date_col}' não encontrada para criar 'ANO'.")
    df_copia['ANO'] = pd.to_datetime(df_copia[date_col], errors='coerce').dt.year
    return df_copia
