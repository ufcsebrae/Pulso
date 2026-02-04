# processamento/processamento_dados_base.py (VERSÃO MAIS ROBUSTA)
import logging
import os
import sys
import pandas as pd

try:
    from config.logger_config import configurar_logger
    from config.inicializacao import carregar_drivers_externos
except ImportError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.warning("Não foi possível importar logger_config ou inicializacao.")
    def configurar_logger(name): return logging.getLogger(name)
    def carregar_drivers_externos(): pass

logger = logging.getLogger(__name__)

try:
    from config.config import CONFIG
except ImportError:
    logger.critical("Erro: O arquivo 'config.py' não foi encontrado.")
    sys.exit(1)

from config.database import get_conexao

def formatar_brl(valor):
    if pd.isna(valor) or valor == 0: return "R$ 0"
    if abs(valor) >= 1_000_000: return f"R$ {(valor / 1_000_000):.2f} M"
    if abs(valor) >= 1_000: return f"R$ {(valor / 1_000):.1f} k"
    return f"R$ {valor:,.2f}"

def carregar_mapas_padronizacao() -> tuple[dict, dict]:
    logger.info("Carregando arquivos de mapeamento para padronização...")
    mapa_unidade, mapa_natureza = {}, {}
    try:
        # Carregamento do mapa de unidades (sem alteração)
        unidade_csv_path = CONFIG.paths.unidade_csv
        if unidade_csv_path.exists():
            df_unidade = pd.read_csv(unidade_csv_path, sep=';', encoding='utf-8-sig', on_bad_lines='warn')
            df_unidade['nm_unidade_padronizada_std'] = df_unidade['nm_unidade_padronizada'].astype(str).str.strip().str.upper()
            mapa_unidade = pd.Series(df_unidade['final'].values, index=df_unidade['nm_unidade_padronizada_std']).to_dict()
            logger.info("Mapa de unidades carregado com %d regras.", len(mapa_unidade))
        else:
            logger.warning("Arquivo 'UNIDADE.CSV' não encontrado.")

        # --- LÓGICA DE CARREGAMENTO DO MAPA DE NATUREZAS TORNADA MAIS ROBUSTA ---
        natureza_csv_path = CONFIG.paths.natureza_csv
        if natureza_csv_path.exists():
            df_natureza = pd.read_csv(natureza_csv_path, sep=';', encoding='utf-8-sig', on_bad_lines='warn')

            # 1. Validação explícita das colunas obrigatórias
            required_cols = ['Descricao_Natureza_Orcamentaria', 'Descricao_Natureza_Orcamentaria_FINAL']
            if not all(col in df_natureza.columns for col in required_cols):
                missing_cols = [col for col in required_cols if col not in df_natureza.columns]
                raise KeyError(f"O arquivo 'NATUREZA.csv' não contém as colunas obrigatórias: {missing_cols}. Verifique os nomes das colunas no arquivo.")
            
            # Processamento continua se as colunas existirem
            df_natureza['Descricao_Natureza_Orcamentaria_std'] = df_natureza['Descricao_Natureza_Orcamentaria'].astype(str).str.strip().str.upper()
            df_natureza.drop_duplicates(subset=['Descricao_Natureza_Orcamentaria_std'], keep='last', inplace=True)
            
            # Forma mais segura e idiomática de criar o dicionário de mapeamento
            mapa_natureza = df_natureza.set_index('Descricao_Natureza_Orcamentaria_std')['Descricao_Natureza_Orcamentaria_FINAL'].to_dict()
            
            logger.info("Mapa de naturezas carregado com %d regras.", len(mapa_natureza))
        else:
            logger.warning("Arquivo 'NATUREZA.csv' não encontrado.")
            
    except Exception as e:
        # 2. Log de erro melhorado para capturar o traceback completo
        logger.exception("Falha crítica ao carregar os mapas de padronização.")
        # Relança a exceção para interromper a execução, já que é um passo crítico
        raise e
        
    return mapa_unidade, mapa_natureza

def obter_dados_processados() -> pd.DataFrame | None:
    # Esta função permanece com a lógica de chamar a função do banco de dados
    try:
        configurar_logger("processamento_base.log")
        carregar_drivers_externos()
        
        mapa_unidade, _ = carregar_mapas_padronizacao()

        engine_db = get_conexao(CONFIG.conexoes["FINANCA_SQL"])
        PPA_FILTRO = os.getenv("PPA_FILTRO", 'PPA 2025 - 2025/DEZ')
        ANO_FILTRO = int(os.getenv("ANO_FILTRO", 2025))
        
        sql_query = "SELECT * FROM dbo.vw_Analise_Planejado_vs_Executado_v2(?, ?, ?)"
        params = (f'{ANO_FILTRO}-01-01', f'{ANO_FILTRO}-12-31', PPA_FILTRO)
        
        logger.info("Carregando dados base via função 'dbo.vw_Analise_Planejado_vs_Executado_v2'...")
        df_base = pd.read_sql(sql_query, engine_db, params=params)
        logger.info("%d linhas carregadas.", len(df_base))

        if df_base.empty:
            logger.warning("A consulta não retornou dados.")
            return df_base

        logger.info("Iniciando padronização e categorização dos dados...")
        
        df_base['nm_unidade_padronizada'] = df_base['UNIDADE'].astype(str).str.replace('SP - ', '', regex=False).str.strip().str.upper()
        df_base['UNIDADE_FINAL'] = df_base['nm_unidade_padronizada'].map(mapa_unidade).fillna(df_base['nm_unidade_padronizada'])
        
        unidades_por_projeto = df_base.groupby('PROJETO')['nm_unidade_padronizada'].nunique()
        df_base['tipo_projeto'] = df_base['PROJETO'].map(unidades_por_projeto).apply(lambda x: 'Compartilhado' if x > 1 else 'Exclusivo')
        
        colunas_para_remover = ['UNIDADE', 'nm_unidade_padronizada']
        df_base.drop(columns=[col for col in colunas_para_remover if col in df_base.columns], inplace=True)
        
        logger.info("Processamento da base de dados (Python) concluído.")
        return df_base

    except Exception as e:
        logger.exception(f"Falha crítica no processamento da base de dados: {e}")
        return None
