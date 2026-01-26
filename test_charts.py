# test_charts.py
import sys
import logging
import pandas as pd
import plotly.graph_objects as go

# --- Bloco de Inicialização Crítica (Copiado de gerar_relatorio.py) ---
try:
    from config.logger_config import configurar_logger
    configurar_logger("test_script.log")
    from config.inicializacao import carregar_drivers_externos
    carregar_drivers_externos()
except (ImportError, FileNotFoundError) as e:
    logging.basicConfig(level=logging.INFO)
    logging.critical("Falha gravíssima na inicialização: %s", e, exc_info=True)
    sys.exit(1)

from processamento.processamento_dados_base import obter_dados_processados
from visualizacao.preparadores_dados import (
    preparar_dados_orcamento_ocioso,
    preparar_dados_execucao_sem_planejamento
)

def run_test():
    """
    Executa o teste de visualização para uma unidade específica.
    """
    print("--- INICIANDO TESTE DE VISUALIZAÇÃO EM PYTHON ---")
    
    # 1. Carrega todos os dados
    print("Carregando base de dados completa...")
    df_base_total = obter_dados_processados()
    if df_base_total is None or df_base_total.empty:
        print("ERRO: A base de dados não pôde ser carregada. Encerrando teste.")
        return
    print("Base de dados carregada com sucesso.")

    # 2. Isola os dados de uma unidade de teste
    UNIDADE_TESTE = 'ATENDIMENTO AO CLIENTE'
    print(f"\nFiltrando dados para a unidade: '{UNIDADE_TESTE}'...")
    df_unidade = df_base_total[df_base_total['UNIDADE_FINAL'] == UNIDADE_TESTE].copy()
    
    if df_unidade.empty:
        print(f"ERRO: Nenhum dado encontrado para a unidade '{UNIDADE_TESTE}'.")
        return
        
    df_exclusivos = df_unidade[df_unidade['tipo_projeto'] == 'Exclusivo'].copy()
    df_compartilhados = df_unidade[df_unidade['tipo_projeto'] == 'Compartilhado'].copy()
    print("Dados da unidade filtrados.")

    # --------------------------------------------------------------------
    # TESTE 1: Orçamento Ocioso
    # --------------------------------------------------------------------
    print("\n--- TESTE 1: Gerando dados de 'Orçamento Ocioso' ---")
    dados_ocioso = preparar_dados_orcamento_ocioso(df_unidade)
    
    if dados_ocioso and dados_ocioso.get("labels"):
        print("DADOS ENCONTRADOS! Tentando gerar gráfico...")
        print(dados_ocioso)
        fig1 = go.Figure()
        fig1.add_trace(go.Bar(name='Exclusivo', x=dados_ocioso['labels'], y=dados_ocioso['values_exclusivo']))
        fig1.add_trace(go.Bar(name='Compartilhado', x=dados_ocioso['labels'], y=dados_ocioso['values_compartilhado']))
        fig1.update_layout(barmode='stack', title="TESTE PYTHON: Orçamento Ocioso por Projeto")
        fig1.show() # <-- Isso deve abrir uma janela no seu navegador
        print("GRÁFICO 1 EXIBIDO. Verifique a janela do navegador.")
    else:
        print("RESULTADO: Sem dados de 'Orçamento Ocioso' retornados pela função.")

    # --------------------------------------------------------------------
    # TESTE 2: Execução Sem Planejamento (Exclusivos)
    # --------------------------------------------------------------------
    print("\n--- TESTE 2: Gerando dados de 'Execução Sem Planejamento (Exclusivos)' ---")
    dados_sem_plan_exc = preparar_dados_execucao_sem_planejamento(df_exclusivos, 'Exclusivo')

    if dados_sem_plan_exc and dados_sem_plan_exc.get("labels"):
        print("DADOS ENCONTRADOS! Tentando gerar gráfico...")
        print(dados_sem_plan_exc)
        fig2 = go.Figure(data=[go.Bar(x=dados_sem_plan_exc['labels'], y=dados_sem_plan_exc['values'])])
        fig2.update_layout(title="TESTE PYTHON: Execução Sem Planejamento (Exclusivos)")
        fig2.show()
        print("GRÁFICO 2 EXIBIDO. Verifique a janela do navegador.")
    else:
        print("RESULTADO: Sem dados de 'Execução Sem Planejamento (Exclusivos)' retornados pela função.")

    # --------------------------------------------------------------------
    # TESTE 3: Execução Sem Planejamento (Compartilhados)
    # --------------------------------------------------------------------
    print("\n--- TESTE 3: Gerando dados de 'Execução Sem Planejamento (Compartilhados)' ---")
    dados_sem_plan_comp = preparar_dados_execucao_sem_planejamento(df_compartilhados, 'Compartilhado')

    if dados_sem_plan_comp and dados_sem_plan_comp.get("labels"):
        print("DADOS ENCONTRADOS! Tentando gerar gráfico...")
        print(dados_sem_plan_comp)
        fig3 = go.Figure(data=[go.Bar(x=dados_sem_plan_comp['labels'], y=dados_sem_plan_comp['values'])])
        fig3.update_layout(title="TESTE PYTHON: Execução Sem Planejamento (Compartilhados)")
        fig3.show()
        print("GRÁFICO 3 EXIBIDO. Verifique a janela do navegador.")
    else:
        print("RESULTADO: Sem dados de 'Execução Sem Planejamento (Compartilhados)' retornados pela função.")

    print("\n--- TESTE CONCLUÍDO ---")

if __name__ == "__main__":
    run_test()
