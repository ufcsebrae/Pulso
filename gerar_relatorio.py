# gerar_relatorio.py (VERSÃO FINAL COM INJEÇÃO DE CORES)
import argparse
import logging
import sys
import json
import pandas as pd

try:
    from config.logger_config import configurar_logger
    configurar_logger("geracao_relatorio.log")
    from config.inicializacao import carregar_drivers_externos
    carregar_drivers_externos()
except (ImportError, FileNotFoundError) as e:
    logging.basicConfig(level=logging.INFO)
    logging.critical("Falha gravíssima na inicialização: %s", e, exc_info=True)
    sys.exit(1)

from processamento.processamento_dados_base import obter_dados_processados
from comunicacao.enviar_relatorios import carregar_gerentes_do_csv
# Importando CORES junto com CONFIG
from config.config import CONFIG, CORES
from visualizacao.componentes_plotly import criar_grafico_sunburst, criar_grafico_heatmap, criar_grafico_inercia
from visualizacao.preparadores_dados import (
    preparar_dados_kpi,
    preparar_dados_grafico_tendencia,
    preparar_dados_treemap,
    preparar_dados_orcamento_ocioso,
    preparar_dados_execucao_sem_planejamento
)

logger = logging.getLogger(__name__)

def gerar_relatorio_para_unidade(unidade_antiga: str, unidade_nova: str, df_base_total: pd.DataFrame):
    logger.info(f"Iniciando a geração do dashboard para: '{unidade_nova}' (dados de: '{unidade_antiga}')...")
    df_unidade = df_base_total[df_base_total['UNIDADE_FINAL'] == unidade_antiga].copy()
    if df_unidade.empty:
        logger.warning(f"Nenhum dado encontrado para a unidade '{unidade_antiga}'. Relatório não gerado.")
        return

    df_exclusivos = df_unidade[df_unidade['tipo_projeto'] == 'Exclusivo'].copy()
    df_compartilhados = df_unidade[df_unidade['tipo_projeto'] == 'Compartilhado'].copy()

    kpi_dict = preparar_dados_kpi(df_unidade, df_exclusivos, df_compartilhados, unidade_nova)
    
    dados_graficos_json = {
        "trend": preparar_dados_grafico_tendencia(df_unidade),
        "treemap_exclusivo": preparar_dados_treemap(df_exclusivos),
        "treemap_compartilhado": preparar_dados_treemap(df_compartilhados),
        "idle_budget": preparar_dados_orcamento_ocioso(df_unidade),
        "unplanned_exclusivo": preparar_dados_execucao_sem_planejamento(df_exclusivos, 'Exclusivo'),
        "unplanned_compartilhado": preparar_dados_execucao_sem_planejamento(df_compartilhados, 'Compartilhado'),
        # --- CORREÇÃO APLICADA AQUI: Injetando as cores no JSON ---
        "cores": CORES
    }

    placeholders_html = {
        "__SUNBURST_PLACEHOLDER__": criar_grafico_sunburst(df_exclusivos),
        "__HEATMAP_PLACEHOLDER__": criar_grafico_heatmap(df_exclusivos),
        "__INERCIA_PLACEHOLDER__": criar_grafico_inercia(df_exclusivos)
    }

    try:
        template_path = CONFIG.paths.templates_dir / "dashboard_template.html"
        final_html = template_path.read_text(encoding='utf-8')

        for key, value in {**kpi_dict, **placeholders_html}.items():
            final_html = final_html.replace(key, str(value))
        
        json_string = json.dumps(dados_graficos_json, indent=None, ensure_ascii=False)
        final_html = final_html.replace('<!--__JSON_DATA_PLACEHOLDER__-->', json_string)
        
        output_sanitized_name = unidade_nova.replace(' ', '_').replace('/', '_')
        output_path = CONFIG.paths.docs_dir / f"dashboard_{output_sanitized_name}.html"
        output_path.write_text(final_html, encoding='utf-8')
        
        logger.info(f"Dashboard para '{unidade_nova}' salvo com sucesso em: '{output_path}'")
    except Exception as e:
        logger.exception(f"Ocorreu um erro ao gerar o HTML para '{unidade_nova}': {e}")

def selecionar_unidades_interativamente(unidades_map: dict) -> list[str]:
    if not unidades_map: return []
    lista_exibicao = sorted([(v['nome_novo'], k) for k, v in unidades_map.items()])
    print("\n--- Unidades Disponíveis para Geração de Relatório ---")
    for i, (nome_novo, _) in enumerate(lista_exibicao, 1): print(f"  {i:2d}) {nome_novo}")
    print("  all) Gerar para todas as unidades"); print("-" * 55)
    while True:
        escolha_str = input("Escolha os números (ex: 1, 3, 5), 'all' ou enter para sair: ").strip()
        if not escolha_str: return []
        if escolha_str.lower() == 'all': return [item[1] for item in lista_exibicao]
        try:
            indices = [int(num.strip()) - 1 for num in escolha_str.split(',')]
            return [lista_exibicao[i][1] for i in indices if 0 <= i < len(lista_exibicao)]
        except (ValueError, IndexError): print("Entrada inválida.")

def main():
    parser = argparse.ArgumentParser(description="Gera dashboards de performance orçamentária por unidade.")
    parser.add_argument("--unidade", type=str, help="Gera o dashboard para uma unidade específica (usar o nome novo).")
    parser.add_argument("--todas", action="store_true", help="Gera relatórios para todas as unidades disponíveis.")
    args = parser.parse_args()

    CONFIG.paths.docs_dir.mkdir(parents=True, exist_ok=True)
    df_base_total = obter_dados_processados()
    if df_base_total is None or df_base_total.empty:
        logger.error("A base de dados não pôde ser carregada. Encerrando."); sys.exit(1)
        
    gerentes_info = carregar_gerentes_do_csv()
    if not gerentes_info:
        logger.error("Arquivo de gerentes não pôde ser carregado. Encerrando."); sys.exit(1)

    unidades_antigas_disponiveis = df_base_total['UNIDADE_FINAL'].unique()
    unidades_map = { nome_antigo: gerentes_info.get(nome_antigo.upper(), {'nome_novo': nome_antigo.replace("UNIDADE ", "").strip()}) for nome_antigo in unidades_antigas_disponiveis }

    unidades_a_gerar_chaves = []
    if args.unidade:
        nome_novo_arg = args.unidade.upper()
        chave_encontrada = next((k for k, v in unidades_map.items() if v['nome_novo'].upper() == nome_novo_arg), None)
        if chave_encontrada: unidades_a_gerar_chaves = [chave_encontrada]
        else: logger.error(f"Unidade '{args.unidade}' não encontrada no mapeamento.")
    elif args.todas:
        unidades_a_gerar_chaves = list(unidades_map.keys())
    else:
        unidades_a_gerar_chaves = selecionar_unidades_interativamente(unidades_map)

    if unidades_a_gerar_chaves:
        logger.info(f"Gerando dashboards para: {', '.join([unidades_map[k]['nome_novo'] for k in unidades_a_gerar_chaves])}")
        for chave_antiga in unidades_a_gerar_chaves:
            nome_novo = unidades_map[chave_antiga]['nome_novo']
            gerar_relatorio_para_unidade(chave_antiga, nome_novo, df_base_total)
    else:
        logger.info("Nenhuma unidade selecionada. Encerrando.")
    
    logger.info("\n--- FIM DO SCRIPT DE GERAÇÃO DE DASHBOARD ---")

if __name__ == "__main__":
    main()
