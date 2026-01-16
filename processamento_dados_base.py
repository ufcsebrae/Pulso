# processamento_dados_base.py
import logging
import os
import sys
from pathlib import Path
import pandas as pd
import numpy as np

# Garante que o logger e os drivers sejam carregados primeiro
try:
    from logger_config import configurar_logger
    configurar_logger("processamento_base.log")
    from inicializacao import carregar_drivers_externos
    carregar_drivers_externos()
except (ImportError, FileNotFoundError) as e:
    logging.basicConfig(level=logging.INFO)
    logging.critical(f"Falha na inicialização do módulo de dados: {e}")
    sys.exit(1)

# Importações do projeto
from config import CONFIG
from database import get_conexao

logger = logging.getLogger(__name__)

def carregar_mapas_padronizacao() -> tuple[dict, dict]:
    """
    Carrega os arquivos CSV de mapeamento para Unidades e Naturezas,
    com tratamento para linhas malformadas.
    """
    logger.info("Carregando arquivos de mapeamento para padronização...")
    mapa_unidade, mapa_natureza = {}, {}

    try:
        # Carrega o mapa de Unidades
        df_unidade = pd.read_csv(
            "UNIDADE.CSV", 
            sep=';', 
            encoding='utf-8-sig',
            on_bad_lines='warn' # Adiciona tolerância a erros
        )
        mapa_unidade = pd.Series(df_unidade['final'].values, index=df_unidade['nm_unidade_padronizada']).to_dict()
        logger.info("Mapa de unidades carregado com %d regras.", len(mapa_unidade))

        # Carrega o mapa de Naturezas (provável fonte do erro)
        logger.info("Tentando carregar 'NATUREZA.csv' com tratamento de erros...")
        df_natureza = pd.read_csv(
            "NATUREZA.csv", 
            sep=';', 
            encoding='utf-8-sig',
            on_bad_lines='warn' # AVISA sobre linhas com erro em vez de parar
        )
        
        # Confirma se as colunas esperadas existem após o carregamento
        if 'Descricao_Natureza_Orcamentaria' in df_natureza.columns and 'Descricao_Natureza_Orcamentaria_FINAL' in df_natureza.columns:
            df_natureza.drop_duplicates(subset=['Descricao_Natureza_Orcamentaria'], keep='last', inplace=True)
            mapa_natureza = pd.Series(
                df_natureza['Descricao_Natureza_Orcamentaria_FINAL'].values,
                index=df_natureza['Descricao_Natureza_Orcamentaria']
            ).to_dict()
            logger.info("Mapa de naturezas carregado com %d regras.", len(mapa_natureza))
        else:
            logger.error("As colunas esperadas não foram encontradas em 'NATUREZA.csv'. Verifique o separador (;) e o cabeçalho do arquivo.")

    except FileNotFoundError as e:
        logger.warning("Arquivo de mapa não encontrado: %s. A padronização pode falhar.", e.filename)
    except Exception as e:
        logger.error("Falha crítica ao carregar os mapas de padronização: %s", e)

    return mapa_unidade, mapa_natureza

def obter_dados_processados() -> pd.DataFrame | None:
    """
    Função central que busca, processa, padroniza e categoriza os dados,
    servindo como base única para todos os relatórios.
    """
    # 1. Carregar mapas
    mapa_unidade, mapa_natureza = carregar_mapas_padronizacao()

    # 2. Conectar ao BD
    try:
        engine_db = get_conexao(CONFIG.conexoes["FINANCA_SQL"])
    except Exception as e:
        logger.critical(f"Falha crítica ao conectar ao banco de dados: {e}")
        return None

    # 3. Carregar dados brutos
    PPA_FILTRO = os.getenv("PPA_FILTRO", 'PPA 2025 - 2025/DEZ')
    ANO_FILTRO = int(os.getenv("ANO_FILTRO", 2025))
    sql_query = "SELECT * FROM dbo.vw_Analise_Planejado_vs_Executado_v2(?, ?, ?)"
    params = (f'{ANO_FILTRO}-01-01', f'{ANO_FILTRO}-12-31', PPA_FILTRO)
    
    logger.info("Carregando dados base da view '%s'...", "vw_Analise_Planejado_vs_Executado_v2")
    try:
        df_base = pd.read_sql(sql_query, engine_db, params=params)
        logger.info("%d linhas carregadas.", len(df_base))
    except Exception as e:
        logger.critical("Não foi possível carregar os dados da view. Erro: %s", e)
        return None

    if df_base.empty:
        logger.warning("A consulta não retornou dados.")
        return df_base

    # 4. Padronizações e Categorizações
    logger.info("Iniciando padronização e categorização dos dados...")
    
    df_base['nm_unidade_padronizada'] = df_base['UNIDADE'].str.upper().str.replace('SP - ', '', regex=False).str.strip()
    
    if mapa_unidade:
        df_base['UNIDADE_FINAL'] = df_base['nm_unidade_padronizada'].map(mapa_unidade).fillna(df_base['nm_unidade_padronizada'])
    else:
        df_base['UNIDADE_FINAL'] = df_base['nm_unidade_padronizada']

    if mapa_natureza:
        df_base['NATUREZA_FINAL'] = df_base['Descricao_Natureza_Orcamentaria'].map(mapa_natureza).fillna(df_base['Descricao_Natureza_Orcamentaria'])
    else:
        df_base['NATUREZA_FINAL'] = df_base['Descricao_Natureza_Orcamentaria']

    unidades_por_projeto = df_base.groupby('PROJETO')['nm_unidade_padronizada'].nunique().reset_index()
    unidades_por_projeto.rename(columns={'nm_unidade_padronizada': 'contagem_unidades'}, inplace=True)
    unidades_por_projeto['tipo_projeto'] = np.where(unidades_por_projeto['contagem_unidades'] > 1, 'Compartilhado', 'Exclusivo')
    
    df_final = pd.merge(df_base, unidades_por_projeto[['PROJETO', 'tipo_projeto']], on='PROJETO', how='left')

    logger.info("Processamento da base de dados concluído.")
    return df_final
