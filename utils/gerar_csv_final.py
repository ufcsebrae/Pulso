# gerar_csv_final.py
import logging
import sys
import pandas as pd

# --- Módulo de Dados Centralizado ---
# Garante que a base de dados seja sempre a mesma em qualquer script
try:
    from processamento_dados_base import obter_dados_processados
except ImportError:
    logging.basicConfig(level=logging.INFO)
    logging.critical("Erro: O arquivo 'processamento_dados_base.py' não foi encontrado.")
    sys.exit(1)
    
from config import CONFIG

logger = logging.getLogger(__name__)

def gerar_csv_final():
    """
    Busca os dados processados do módulo central e salva em um arquivo CSV.
    """
    logger.info("Iniciando a geração do arquivo CSV a partir da base de dados centralizada...")

    # 1. Obter a base de dados já processada e padronizada
    df_final = obter_dados_processados()

    if df_final is None or df_final.empty:
        logger.error("A base de dados não pôde ser carregada. O arquivo CSV não será gerado.")
        sys.exit(1)

    # 2. Salvando o Arquivo CSV
    output_filename = "relatorio_final.csv"
    output_path = CONFIG.paths.base_dir / output_filename

    try:
        logger.info(f"Salvando o arquivo em '{output_path}'...")
        # Formatação ideal para abrir no Excel em português
        df_final.to_csv(output_path, index=False, sep=';', decimal=',', encoding='utf-8-sig')
        logger.info("Arquivo '%s' gerado com sucesso na raiz do projeto!", output_filename)
        print(f"\nSUCESSO! O arquivo '{output_filename}' foi gerado na pasta do projeto.")
    except Exception as e:
        logger.error("Falha ao salvar o arquivo CSV. Erro: %s", e)

if __name__ == "__main__":
    gerar_csv_final()

